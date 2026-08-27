// Package realtime 提供 Agent 到 Worker 的独立上行 WebSocket。
//
// 采集节奏与发送节奏是解耦的：每次采集都进缓冲，但每 live-interval 才发一批，
// 且只在服务端说「有人在看」时才发。发送频率必须与采集间隔无关——Durable
// Object 的每条入站 WebSocket 消息都单独计一次 Worker 请求，秒级一帧时单台
// 探针就是 86400 次/天，而其中绝大多数帧是推给空房间的。
// 暂停与断线期间缓冲照常写入但有界，超出丢最旧的样本；完整历史仍由调用方先
// 写入本地 spool，并按固定周期通过 HTTP 批量投递，不受实时通道任何影响。
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xugou/agent/pkg/config"
	"github.com/xugou/agent/pkg/model"
)

const (
	liveProtocolVersion = 2
	reconnectMinDelay   = time.Second
	reconnectMaxDelay   = 30 * time.Second
	pingInterval        = 25 * time.Second
	writeTimeout        = 5 * time.Second
	readLimitBytes      = 64 * 1024

	// DefaultLiveInterval 是攒批发送的默认间隔。12 秒把单台探针的实时链路
	// 从 86400 次/天压到 7200 次/天，代价只是实时视图最多晚 12 秒。
	DefaultLiveInterval = 12 * time.Second

	// maxBatchBytes 单批序列化上限。服务端 MAX_LIVE_FRAME_BYTES 是 64 KB，
	// 这里只用一半：网卡多的机器提前发一批，而不是把帧顶到被 close(1009)。
	maxBatchBytes = 32 * 1024

	// maxBatchSamples 单批样本数上限，必须与服务端 MAX_REPORT_SAMPLES 一致，
	// 超出会被 zod 判为非法帧并 close(1008)。
	maxBatchSamples = 100
)

// pendingSample 缓存样本及其序列化后的字节数，避免攒批时反复 Marshal 估算体积。
type pendingSample struct {
	sample *model.LiveMetricSample
	bytes  int
}

// Client 维护单条可自动重连的上行连接。Publish 始终为非阻塞调用。
type Client struct {
	serverURL    string
	token        string
	agentVersion string
	liveInterval time.Duration
	dialer       websocket.Dialer

	// flush 让「缓冲触顶」和「恢复推流」能立刻打断攒批窗口，容量 1 且只做非阻塞投递。
	flush chan struct{}

	mu             sync.Mutex
	streaming      bool
	pending        []pendingSample
	pendingBytes   int
	sequence       uint64
	previousAt     time.Time
	previousRx     uint64
	previousTx     uint64
	hasPreviousNet bool
}

// NewClient 根据 Agent 的 HTTP Server URL 构造对应的 ws/wss 上行客户端。
// liveInterval <= 0 时退回 DefaultLiveInterval。
func NewClient(
	serverURL, token, agentVersion, proxyURL string,
	liveInterval time.Duration,
) (*Client, error) {
	if _, err := buildWebSocketURL(serverURL); err != nil {
		return nil, err
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	if strings.TrimSpace(proxyURL) != "" {
		proxy, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("解析 WebSocket 代理 URL 失败: %w", err)
		}
		dialer.Proxy = http.ProxyURL(proxy)
	}
	if liveInterval <= 0 {
		liveInterval = DefaultLiveInterval
	}
	return &Client{
		serverURL:    serverURL,
		token:        token,
		agentVersion: agentVersion,
		liveInterval: liveInterval,
		dialer:       dialer,
		flush:        make(chan struct{}, 1),
		// 默认常开：服务端不支持按需推流（或指令丢了）时宁可多发，也不能静默。
		// 状态跨重连保留，避免暂停期间每次重连都先倒一批没人看的数据出去。
		streaming: true,
	}, nil
}

func buildWebSocketURL(serverURL string) (string, error) {
	base, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil || base.Host == "" {
		return "", fmt.Errorf("实时服务器 URL 非法: %q", serverURL)
	}
	switch strings.ToLower(base.Scheme) {
	case "http":
		base.Scheme = "ws"
	case "https":
		base.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("实时服务器 URL 协议非法: %q", base.Scheme)
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/v2/agents/live"
	base.RawPath = ""
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
}

// Publish 把一次采集追加进待发缓冲，由发送循环按 live-interval 攒批发出。
// 网络状态不会阻塞采集：缓冲有界，满了丢最旧的样本而不是阻塞调用方。
func (c *Client) Publish(info *model.SystemInfo) {
	if info == nil {
		return
	}
	c.mu.Lock()
	rx, tx, hasNetwork := sumNetworkTotals(info.Network)
	var rxSpeed, txSpeed *float64
	if hasNetwork && c.hasPreviousNet && info.Timestamp.After(c.previousAt) {
		seconds := info.Timestamp.Sub(c.previousAt).Seconds()
		if rx >= c.previousRx {
			value := float64(rx-c.previousRx) / seconds
			rxSpeed = &value
		}
		if tx >= c.previousTx {
			value := float64(tx-c.previousTx) / seconds
			txSpeed = &value
		}
	}
	if hasNetwork {
		c.previousAt = info.Timestamp
		c.previousRx = rx
		c.previousTx = tx
		c.hasPreviousNet = true
	}
	sample := &model.LiveMetricSample{
		CollectedAt:    info.Timestamp.UTC().Format(time.RFC3339Nano),
		CPU:            info.CPU,
		Memory:         info.Memory,
		Load:           info.Load,
		Network:        append([]model.NetworkInfo(nil), info.Network...),
		Swap:           info.Swap,
		NetworkRxSpeed: rxSpeed,
		NetworkTxSpeed: txSpeed,
	}
	payload, err := json.Marshal(sample)
	if err != nil {
		c.mu.Unlock()
		log.Printf("序列化实时样本失败，跳过本轮实时发布: %v", err)
		return
	}
	// 单个样本就超过整批上限的机器发不出去（服务端会判非法帧并断连），
	// 与其让整条连接反复重连，不如在这里就放弃这一个样本。
	if len(payload) > maxBatchBytes {
		c.mu.Unlock()
		log.Printf("实时样本 %d 字节超过单批上限 %d，跳过", len(payload), maxBatchBytes)
		return
	}

	c.pending = append(c.pending, pendingSample{sample: sample, bytes: len(payload)})
	c.pendingBytes += len(payload)
	c.trimPendingLocked()
	// 缓冲已经能装满一批，不必等攒批窗口走完。暂停期间缓冲长期处于满档，
	// 这里不加 streaming 判断的话每次采集都会白白唤醒一次发送循环。
	overflow := c.streaming &&
		(c.pendingBytes >= maxBatchBytes || len(c.pending) >= maxBatchSamples)
	c.mu.Unlock()

	if overflow {
		c.signalFlush()
	}
}

// trimPendingLocked 丢弃超出容量的最旧样本。实时通道只负责「新」，
// 断线期间的历史由 spool + HTTP 批量补齐，堆在内存里没有意义。
func (c *Client) trimPendingLocked() {
	for len(c.pending) > maxBatchSamples {
		c.pendingBytes -= c.pending[0].bytes
		c.pending = c.pending[1:]
	}
}

func (c *Client) signalFlush() {
	select {
	case c.flush <- struct{}{}:
	default:
	}
}

// controlFrame 是服务端下行的控制帧。指标帧是单向上行的，下行只有控制。
type controlFrame struct {
	Type string `json:"type"`
	// Stream 出现在 hello 里，表示当前是否有人在看。旧服务端不带这个字段。
	Stream *bool `json:"stream"`
	// Enabled 出现在 stream 指令里，订阅者从 0 变正 / 从正变 0 时下发。
	Enabled *bool `json:"enabled"`
}

// applyControl 处理服务端下行的推流开关。无法解析或不认识的帧一律忽略——
// 控制通道出问题时应该退化成「照常推流」，而不是静默。
func (c *Client) applyControl(data []byte) {
	var control controlFrame
	if err := json.Unmarshal(data, &control); err != nil {
		return
	}
	var enabled bool
	switch control.Type {
	case "hello":
		// 旧服务端的 hello 不带 stream：视为不支持按需推流，恢复常开。
		// 少了这一条，服务端一旦回滚，暂停中的探针会永远静默下去。
		enabled = control.Stream == nil || *control.Stream
	case "stream":
		if control.Enabled == nil {
			return
		}
		enabled = *control.Enabled
	default:
		return
	}

	c.mu.Lock()
	changed := c.streaming != enabled
	c.streaming = enabled
	c.mu.Unlock()
	if !changed {
		return
	}
	if enabled {
		log.Printf("实时推流已恢复：有订阅者在看")
		// 立刻补发缓冲里的样本，别让刚打开页面的人等一个攒批窗口。
		c.signalFlush()
		return
	}
	log.Printf("实时推流已暂停：当前无人订阅")
}

func (c *Client) isStreaming() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streaming
}

// takeBatch 取出一批不超过字节/条数上限的样本，剩余的留到下一批。
func (c *Client) takeBatch() []pendingSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	total := 0
	taken := 0
	for _, item := range c.pending {
		if taken > 0 && (total+item.bytes > maxBatchBytes || taken >= maxBatchSamples) {
			break
		}
		total += item.bytes
		taken++
	}
	batch := make([]pendingSample, taken)
	copy(batch, c.pending[:taken])
	c.pending = c.pending[taken:]
	c.pendingBytes -= total
	return batch
}

func sumNetworkTotals(network []model.NetworkInfo) (uint64, uint64, bool) {
	var rx, tx uint64
	hasNetwork := false
	for _, item := range network {
		if isLoopbackInterface(item.Interface) {
			continue
		}
		rx += item.BytesRecv
		tx += item.BytesSent
		hasNetwork = true
	}
	return rx, tx, hasNetwork
}

func isLoopbackInterface(name string) bool {
	value := strings.ToLower(strings.TrimSpace(name))
	if strings.Contains(value, "loopback") {
		return true
	}
	if value == "lo" {
		return true
	}
	if !strings.HasPrefix(value, "lo") || len(value) == 2 {
		return false
	}
	for _, char := range value[2:] {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

// Run 维护连接直到 ctx 结束。鉴权失败、网络抖动和 Worker 重启都会进入有上限的
// 指数退避；spool 与 HTTP 上报由独立主循环继续执行。
func (c *Client) Run(ctx context.Context) {
	delay := reconnectMinDelay
	for ctx.Err() == nil {
		conn, err := c.connect(ctx)
		if err == nil {
			log.Printf("实时 WebSocket 已连接")
			delay = reconnectMinDelay
			err = c.stream(ctx, conn)
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("实时 WebSocket 已断开，将自动重连: %v", err)
		jitter := time.Duration(rand.Int64N(max(int64(delay/4), 1)))
		timer := time.NewTimer(delay + jitter)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		delay = min(delay*2, reconnectMaxDelay)
	}
}

func (c *Client) connect(ctx context.Context) (*websocket.Conn, error) {
	endpoint, err := buildWebSocketURL(c.serverURL)
	if err != nil {
		return nil, err
	}
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+c.token)
	headers.Set(config.HeaderAgentVersion, c.agentVersion)
	conn, response, err := c.dialer.DialContext(ctx, endpoint, headers)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		if response != nil {
			return nil, fmt.Errorf("WebSocket 握手返回 HTTP %d: %w", response.StatusCode, err)
		}
		return nil, err
	}
	conn.SetReadLimit(readLimitBytes)
	return conn, nil
}

func (c *Client) stream(ctx context.Context, conn *websocket.Conn) error {
	defer conn.Close()
	readErr := make(chan error, 1)
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			c.applyControl(data)
		}
	}()

	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()
	batchTicker := time.NewTicker(c.liveInterval)
	defer batchTicker.Stop()

	// 刚连上先把攒着的样本发出去，避免重连后实时视图空白一个攒批窗口。
	if err := c.sendBatch(conn); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			deadline := time.Now().Add(writeTimeout)
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
				deadline,
			)
			return ctx.Err()
		case err := <-readErr:
			return err
		case <-batchTicker.C:
			if err := c.sendBatch(conn); err != nil {
				return err
			}
		case <-c.flush:
			// 缓冲触顶提前发一批，随后重置窗口，避免紧接着又发一批小的。
			if err := c.sendBatch(conn); err != nil {
				return err
			}
			batchTicker.Reset(c.liveInterval)
		case <-pingTicker.C:
			if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				return err
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return err
			}
		}
	}
}

// sendBatch 取出当前缓冲并发出一帧。缓冲为空时不发帧——没有采集就不该产生请求。
//
// 无人订阅时一帧不发，但采集与缓冲照常进行：缓冲上限约 100 秒，恢复推流时
// 立刻补上这一段，打开页面不至于对着空图表等。历史精度与此无关——那条路
// 走本地 spool + HTTP 批量写 D1，跟实时通道完全独立。
func (c *Client) sendBatch(conn *websocket.Conn) error {
	if !c.isStreaming() {
		return nil
	}
	batch := c.takeBatch()
	if len(batch) == 0 {
		return nil
	}
	samples := make([]*model.LiveMetricSample, len(batch))
	for index, item := range batch {
		samples[index] = item.sample
	}
	frame := &model.LiveMetricBatch{
		Type:            "metric_batch",
		ProtocolVersion: liveProtocolVersion,
		Sequence:        c.nextSequence(),
		Samples:         samples,
	}
	if err := writeFrame(conn, frame); err != nil {
		c.requeue(batch)
		return err
	}
	return nil
}

func (c *Client) nextSequence() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sequence++
	return c.sequence
}

func writeFrame(conn *websocket.Conn, frame *model.LiveMetricBatch) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("序列化实时指标失败: %w", err)
	}
	if len(payload) > readLimitBytes {
		return fmt.Errorf("实时指标帧超过 %d 字节", readLimitBytes)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("发送实时指标失败: %w", err)
	}
	return nil
}

// requeue 把发送失败的一批放回缓冲头部，等重连后随下一批一起发。
// 放回后仍按上限裁剪：重连拖得越久，越应该保留新样本而不是旧样本。
func (c *Client) requeue(batch []pendingSample) {
	if len(batch) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	restored := make([]pendingSample, 0, len(batch)+len(c.pending))
	restored = append(restored, batch...)
	restored = append(restored, c.pending...)
	c.pending = restored
	for _, item := range batch {
		c.pendingBytes += item.bytes
	}
	c.trimPendingLocked()
}
