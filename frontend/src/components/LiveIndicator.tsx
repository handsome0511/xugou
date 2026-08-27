import { useTranslation } from "react-i18next";

interface LiveIndicatorProps {
  connected: boolean;
}

/**
 * WebSocket 连接状态小指示（终端风格）：● live / - offline
 *
 * 刻意不显示样本滞后秒数。探针按 live-interval（默认 12 秒）攒批上行，
 * 一批里的样本会被回放摊开，滞后天然在 0 到十几秒之间来回摆；无人订阅时
 * 探针还会暂停推流，滞后会更大。把这个数字放出来只会不停跳动，
 * 既不代表故障也不代表健康。连接状态是二元的，指示也就该是二元的。
 */
const LiveIndicator = ({ connected }: LiveIndicatorProps) => {
  const { t } = useTranslation();
  const label = connected ? t("live.connected") : t("live.disconnected");

  return (
    <span
      className="text-xs tracking-wider"
      style={{
        color: connected ? "var(--accent-green)" : "var(--text-secondary)",
      }}
      title={label}
      aria-live="polite"
    >
      {connected ? "●" : "-"} {label}
    </span>
  );
};

export default LiveIndicator;
