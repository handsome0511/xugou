package config

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gopkg.in/yaml.v3"
)

// 上报请求头常量。SchemaVersion 与 MD5 头仍随每次上报发出，供服务端识别客户端
// 声明的协议版本；v2/v3 那套 application/x-www-form-urlencoded 的配置串下发已被
// 上报响应里的 JSON 取代，对应的 ParseRemoteConfig 与键白名单已删除。
const (
	SchemaVersion      = 3
	HeaderConfigSchema = "X-Agent-Config-Schema"
	HeaderConfigMd5    = "X-Agent-Config-Md5"
	// HeaderAgentVersion 上报请求携带的探针自身版本（服务端据此判断是否下发 update=1）
	HeaderAgentVersion = "X-Agent-Version"

	MinCollectInterval = 1
	MaxCollectInterval = 3600
	MinReportInterval  = 10
	MaxReportInterval  = 3600
	// 实时攒批间隔只是本地参数，不参与配置下发协议，因此不进规范化串与 MD5。
	MinLiveInterval = 1
	MaxLiveInterval = 300
)

// RemoteConfig 服务端下发并通过整体校验后的配置
type RemoteConfig struct {
	CollectInterval int
	ReportInterval  int
	// Update 服务端触发自升级指令（update=1）；不持久化，仅当次生效
	Update bool
}

// ResolveIntervals 应用 CLI 配置优先级中的旧 --interval Alias：只在对应的新参数
// 未通过命令行、环境变量或配置文件显式提供时，Alias 才覆盖该参数。
func ResolveIntervals(
	legacy, collect, report int,
	legacySet, collectSet, reportSet bool,
) (int, int, error) {
	if legacySet {
		if !collectSet {
			collect = legacy
		}
		if !reportSet {
			report = legacy
		}
	}
	if err := ValidateIntervals(collect, report); err != nil {
		return 0, 0, err
	}
	return collect, report, nil
}

// LoadTokenFile 读取权限受限的 Agent Credential 文件。Unix 上拒绝 group/other
// 权限，降低凭据被同机其他账号读取的风险。
func LoadTokenFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("读取凭据文件状态失败: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("凭据文件必须是普通文件")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("凭据文件权限过宽: %o，期望 0600", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("读取凭据文件失败: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" || len(token) > 512 || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("凭据文件内容格式错误")
	}
	return token, nil
}

// ValidateIntervals 校验采集/上报间隔值域：collect 1-3600，report 10-3600 且 >= collect
func ValidateIntervals(collect, report int) error {
	if collect < MinCollectInterval || collect > MaxCollectInterval {
		return fmt.Errorf("collect_interval 超出值域 [%d, %d]: %d", MinCollectInterval, MaxCollectInterval, collect)
	}
	if report < MinReportInterval || report > MaxReportInterval {
		return fmt.Errorf("report_interval 超出值域 [%d, %d]: %d", MinReportInterval, MaxReportInterval, report)
	}
	if report < collect {
		return fmt.Errorf("report_interval(%d) 不能小于 collect_interval(%d)", report, collect)
	}
	return nil
}

// ValidateLiveInterval 校验实时攒批间隔值域：1-300 秒。
func ValidateLiveInterval(live int) error {
	if live < MinLiveInterval || live > MaxLiveInterval {
		return fmt.Errorf("live_interval 超出值域 [%d, %d]: %d", MinLiveInterval, MaxLiveInterval, live)
	}
	return nil
}

// NormalizedConfigString 生成规范化配置串。键按固定顺序、分隔符不可变，
// 服务端会回填客户端声明的 schema_version 对同一格式计算 MD5，两侧必须逐字节一致。
// update 指令键刻意排除在规范化串之外（仅指令通道，不参与 MD5 协商）。
func NormalizedConfigString(collect, report int) string {
	return fmt.Sprintf(
		"collect_interval=%d&report_interval=%d&schema_version=%d",
		collect, report, SchemaVersion,
	)
}

// MD5Hex 计算字符串的 MD5 十六进制小写摘要
func MD5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// CurrentConfigMD5 计算当前内存配置的规范化 MD5（随配置热更新自动变化）
func CurrentConfigMD5() string {
	return MD5Hex(NormalizedConfigString(CollectInterval, ReportInterval))
}

// PersistIntervals 将采集/上报间隔原子写入 YAML 配置文件：
// 先读旧配置合并，再写同目录临时文件 + rename，避免半写状态。
// path 为空时跳过持久化（仅内存生效）。
func PersistIntervals(path string, collect, report int) error {
	if path == "" {
		return nil
	}
	if err := ValidateIntervals(collect, report); err != nil {
		return err
	}

	settings := map[string]any{}
	if raw, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(raw, &settings); err != nil {
			return fmt.Errorf("解析现有配置文件失败: %w", err)
		}
		if settings == nil {
			settings = map[string]any{}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	settings["collect-interval"] = collect
	settings["report-interval"] = report

	data, err := yaml.Marshal(settings)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".xugou-agent-*.yaml.tmp")
	if err != nil {
		return fmt.Errorf("创建临时配置文件失败: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath) // rename 成功后 remove 静默失败，失败路径下清理残留

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("写入临时配置文件失败: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("刷盘临时配置文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("关闭临时配置文件失败: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return fmt.Errorf("设置配置文件权限失败: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("原子替换配置文件失败: %w", err)
	}
	return nil
}
