package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xugou/agent/pkg/model"
)

func sampleAt(second int) *model.SystemInfo {
	info := &model.SystemInfo{
		Timestamp: time.Date(2026, 8, 27, 0, 0, second, 0, time.UTC),
	}
	info.CPU = model.CPUInfo{Usage: float64(second), Cores: 2, ModelName: "test"}
	info.Memory = model.MemoryInfo{Total: 100, Used: 50, Free: 50, UsageRate: 50}
	info.Load = model.LoadInfo{Load1: 0.1, Load5: 0.2, Load15: 0.3}
	info.Network = []model.NetworkInfo{
		{Interface: "eth0", BytesRecv: uint64(second) * 1000, BytesSent: uint64(second) * 500},
	}
	return info
}

func collectedAt(second int) string {
	return sampleAt(second).Timestamp.UTC().Format(time.RFC3339Nano)
}

// liveTestServer 起一个最小实时服务端：收到的指标帧进 frames，
// 往 control 里写的原始字符串会作为控制帧下发给探针。
func liveTestServer(t *testing.T) (chan model.LiveMetricBatch, chan string, *httptest.Server) {
	t.Helper()
	frames := make(chan model.LiveMetricBatch, 64)
	control := make(chan string, 8)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				_, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				if string(data) == "ping" {
					continue
				}
				var batch model.LiveMetricBatch
				if err := json.Unmarshal(data, &batch); err != nil {
					return
				}
				frames <- batch
			}
		}()

		for {
			select {
			case <-done:
				return
			case message := <-control:
				if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
					return
				}
			}
		}
	}))
	return frames, control, server
}

func drainFrames(frames chan model.LiveMetricBatch) []model.LiveMetricBatch {
	var drained []model.LiveMetricBatch
	for {
		select {
		case batch := <-frames:
			drained = append(drained, batch)
		default:
			return drained
		}
	}
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", message)
}

func newTestClient(t *testing.T, serverURL string, liveInterval time.Duration) *Client {
	t.Helper()
	client, err := NewClient(
		strings.Replace(serverURL, "http://", "ws://", 1),
		"token",
		"v-test",
		"",
		liveInterval,
	)
	if err != nil {
		t.Fatalf("构造实时客户端失败: %v", err)
	}
	return client
}

// 攒批的意义全在这条断言上：采集多少次不重要，重要的是发出去几帧。
// Durable Object 的每条入站消息都单独计一次 Worker 请求，一秒一帧时
// 单台探针就是 86400 次/天，免费额度一台用光。
func TestPublishBatchesInsteadOfOneFramePerSample(t *testing.T) {
	frames, _, server := liveTestServer(t)
	defer server.Close()

	client := newTestClient(t, server.URL, 300*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.Run(ctx)

	// 等连接建好，避免把「连上就先发一批」的首帧算进攒批统计。
	time.Sleep(200 * time.Millisecond)

	const published = 20
	for index := range published {
		client.Publish(sampleAt(index))
		time.Sleep(20 * time.Millisecond)
	}
	// 留出最后一个攒批窗口。
	time.Sleep(500 * time.Millisecond)
	cancel()

	batches := drainFrames(frames)
	sampleCount := 0
	for _, batch := range batches {
		sampleCount += len(batch.Samples)
		if batch.Type != "metric_batch" {
			t.Errorf("帧类型应为 metric_batch，实际 %q", batch.Type)
		}
		if batch.ProtocolVersion != liveProtocolVersion {
			t.Errorf("协议版本应为 %d，实际 %d", liveProtocolVersion, batch.ProtocolVersion)
		}
	}

	if sampleCount != published {
		t.Errorf("采样点不应丢失：发布 %d 个，服务端收到 %d 个", published, sampleCount)
	}
	// 400ms 的采集铺在 300ms 的窗口上，理论上 2-3 帧；给足余量只卡住
	// 「退化回一个样本一帧」这条线。
	if len(batches) == 0 || len(batches) > published/2 {
		t.Errorf("攒批未生效：%d 个采样点发了 %d 帧", published, len(batches))
	}
}

// 按需推流的核心断言：服务端说没人看，探针就一帧都不该发。
// 这里断言的是「发了几帧」而不是「缓冲里有没有数据」——省下来的正是请求数。
func TestPausedClientSendsNoFrames(t *testing.T) {
	frames, control, server := liveTestServer(t)
	defer server.Close()

	client := newTestClient(t, server.URL, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go client.Run(ctx)

	// hello 里就说没人看，探针从一开始就不该推。
	control <- `{"type":"hello","protocol_version":1,"agentId":1,"stream":false}`
	waitFor(t, func() bool { return !client.isStreaming() }, "探针应进入暂停状态")

	const published = 10
	for index := range published {
		client.Publish(sampleAt(index))
		time.Sleep(20 * time.Millisecond)
	}
	// 攒批窗口只有 100ms，没暂停的话这段时间足够发好几帧。
	time.Sleep(300 * time.Millisecond)

	if sent := drainFrames(frames); len(sent) != 0 {
		t.Fatalf("暂停期间不应发出任何帧，实际发了 %d 帧", len(sent))
	}

	// 采集没有停：缓冲攒着，恢复时才补得上。
	client.mu.Lock()
	pending := len(client.pending)
	client.mu.Unlock()
	if pending != published {
		t.Fatalf("暂停期间仍应继续缓冲采样，期望 %d 条，实际 %d 条", published, pending)
	}

	// 恢复后要把暂停期间攒的样本补上，而不是从恢复那一刻重新开始。
	control <- `{"type":"stream","enabled":true}`
	recovered := 0
	waitFor(t, func() bool {
		for _, batch := range drainFrames(frames) {
			recovered += len(batch.Samples)
		}
		return recovered >= published
	}, "恢复推流后应补发暂停期间缓冲的样本")

	if recovered != published {
		t.Errorf("恢复后应补齐 %d 个样本，实际 %d 个", published, recovered)
	}
}

// 服务端回滚成不带 stream 字段的旧版本时，暂停中的探针必须自己恢复，
// 否则实时视图会永久静默——这条链路没有别的兜底。
func TestHelloWithoutStreamFieldResumesStreaming(t *testing.T) {
	client := newTestClient(t, "http://example.com", time.Second)

	client.applyControl([]byte(`{"type":"stream","enabled":false}`))
	if client.isStreaming() {
		t.Fatal("收到 stream=false 后应暂停")
	}

	client.applyControl([]byte(`{"type":"hello","protocol_version":1,"agentId":1}`))
	if !client.isStreaming() {
		t.Error("旧服务端的 hello 不带 stream，应视为不支持按需推流并恢复常开")
	}
}

// 控制通道出问题时应该退化成「照常推流」，不能被一条坏帧带进静默。
func TestMalformedControlFramesDoNotPause(t *testing.T) {
	client := newTestClient(t, "http://example.com", time.Second)

	for _, frame := range []string{
		`不是 json`,
		`{"type":"stream"}`,
		`{"type":"unknown","enabled":false}`,
		`{"type":"batchUpdate","updates":[]}`,
	} {
		client.applyControl([]byte(frame))
		if !client.isStreaming() {
			t.Fatalf("控制帧 %q 不应导致暂停", frame)
		}
	}
}

// 缓冲有界：暂停或断线期间不能无限堆积，且要保留最新的样本而不是最旧的。
func TestPendingBufferIsBoundedAndKeepsNewest(t *testing.T) {
	client := newTestClient(t, "http://example.com", time.Second)

	total := maxBatchSamples + 30
	for index := range total {
		client.Publish(sampleAt(index))
	}

	client.mu.Lock()
	pending := len(client.pending)
	oldest := client.pending[0].sample.CollectedAt
	client.mu.Unlock()

	if pending != maxBatchSamples {
		t.Errorf("缓冲应被裁剪到 %d 条，实际 %d 条", maxBatchSamples, pending)
	}
	if expected := collectedAt(total - maxBatchSamples); oldest != expected {
		t.Errorf("裁剪应丢最旧的样本：期望队首 %s，实际 %s", expected, oldest)
	}
}

// takeBatch 必须按采集顺序整批取走，并且不留下已计入的字节数。
func TestTakeBatchDrainsInOrder(t *testing.T) {
	client := newTestClient(t, "http://example.com", time.Second)
	for index := range 5 {
		client.Publish(sampleAt(index))
	}

	batch := client.takeBatch()
	if len(batch) != 5 {
		t.Fatalf("应取走全部 5 个样本，实际 %d 个", len(batch))
	}
	for index, item := range batch {
		if expected := collectedAt(index); item.sample.CollectedAt != expected {
			t.Errorf("第 %d 个样本顺序错乱：期望 %s，实际 %s", index, expected, item.sample.CollectedAt)
		}
	}

	client.mu.Lock()
	pendingBytes := client.pendingBytes
	pending := len(client.pending)
	client.mu.Unlock()
	if pending != 0 || pendingBytes != 0 {
		t.Errorf("取空后缓冲应清零，实际 %d 条 / %d 字节", pending, pendingBytes)
	}

	if client.takeBatch() != nil {
		t.Error("缓冲为空时 takeBatch 应返回 nil，避免发出空帧白白消耗一次请求")
	}
}

// 发送失败的一批要放回队首，等重连后补发，而不是当场丢掉。
func TestRequeueRestoresBatchAtHead(t *testing.T) {
	client := newTestClient(t, "http://example.com", time.Second)
	for index := range 3 {
		client.Publish(sampleAt(index))
	}
	batch := client.takeBatch()
	client.Publish(sampleAt(99))
	client.requeue(batch)

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.pending) != 4 {
		t.Fatalf("放回后应有 4 条，实际 %d 条", len(client.pending))
	}
	if expected := collectedAt(0); client.pending[0].sample.CollectedAt != expected {
		t.Errorf("失败批次应回到队首：期望 %s，实际 %s", expected, client.pending[0].sample.CollectedAt)
	}
}
