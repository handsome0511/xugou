const CREDENTIAL_TOUCH_INTERVAL_MS = 5 * 60 * 1000;

/** 凭据使用时间只记到五分钟粒度；每次认证仍实时检查撤销和节点删除状态。 */
export function shouldTouchCredential(lastUsedAt: string | null, nowMs: number) {
  const lastUsedAtMs = Date.parse(lastUsedAt ?? "");
  return (
    !Number.isFinite(lastUsedAtMs) ||
    nowMs - lastUsedAtMs >= CREDENTIAL_TOUCH_INTERVAL_MS
  );
}
