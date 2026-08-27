/**
 * 按需推流的边沿判定。
 *
 * 探针的实时帧每条都单独计一次 Worker 请求，而没人看 Dashboard 时这些帧只是
 * 写进 AgentRoom 的内存缓存就被丢掉。所以订阅者从 0 变正、从正变 0 时才给
 * 探针下发一次开关指令——**只在边沿下发**：每来一个订阅者就发一条 stream=on
 * 的话，指令本身又变成了新的请求源。
 */
export function streamCommandFor(
  before: number,
  after: number
): boolean | null {
  if (before <= 0 && after > 0) return true;
  if (before > 0 && after <= 0) return false;
  return null;
}
