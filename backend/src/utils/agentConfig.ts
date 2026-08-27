/**
 * 探针协议里与版本、批量上限相关的服务端常量与判定。
 *
 * 历史上这里还承载过 v2/v3 的动态配置协商（X-Agent-Config-Schema/Md5 头 +
 * application/x-www-form-urlencoded 配置串 + 纯 TS MD5）。v4 起配置改由上报响应
 * 的 JSON body 下发，那套协商连同 MD5 实现已随之删除；探针侧对应的
 * ParseRemoteConfig 也一并移除。
 */

// 单次上报最多承载/广播/缓存的样本条数（zod samples 上限、Agent Report Adapter 展开上限、
// realtime publisher 与 AgentRoom 批量上限的单一事实源）
export const MAX_REPORT_SAMPLES = 100;

/**
 * 语义化版本比较（支持 v 前缀与预发布后缀，如 v1.2.3-rc1）。
 * 返回 -1/0/1 表示 a 小于/等于/大于 b；任一版本无法解析时返回 null。
 * 与 Go 侧 selfmgmt.CompareVersions 语义一致（1.0.0 > 1.0.0-rc1）。
 */
export function compareSemver(a: string, b: string): number | null {
  const identifier = /^[0-9A-Za-z-]+$/;
  const parse = (v: string): { core: string[]; pre: string[] } | null => {
    const trimmed = v.trim().replace(/^[vV]/, "");
    if (!trimmed) return null;
    const buildParts = trimmed.split("+");
    if (buildParts.length > 2) return null;
    if (
      buildParts[1] !== undefined &&
      buildParts[1].split(".").some((item) => !identifier.test(item))
    ) {
      return null;
    }
    const dashIndex = buildParts[0].indexOf("-");
    const base = dashIndex < 0 ? buildParts[0] : buildParts[0].slice(0, dashIndex);
    const preValue = dashIndex < 0 ? undefined : buildParts[0].slice(dashIndex + 1);
    const core = base.split(".");
    if (
      core.length !== 3 ||
      core.some((item) => !/^(0|[1-9]\d*)$/.test(item))
    ) {
      return null;
    }
    const pre = preValue === undefined ? [] : preValue.split(".");
    if (
      pre.some(
        (item) =>
          !identifier.test(item) || (/^\d+$/.test(item) && !/^(0|[1-9]\d*)$/.test(item))
      )
    ) {
      return null;
    }
    return { core, pre };
  };

  const va = parse(a);
  const vb = parse(b);
  if (!va || !vb) return null;

  const compareNumeric = (left: string, right: string) =>
    left.length === right.length
      ? left === right
        ? 0
        : left < right
          ? -1
          : 1
      : left.length < right.length
        ? -1
        : 1;
  for (let i = 0; i < 3; i++) {
    const result = compareNumeric(va.core[i], vb.core[i]);
    if (result !== 0) return result;
  }
  if (va.pre.length === 0 && vb.pre.length === 0) return 0;
  if (va.pre.length === 0) return 1;
  if (vb.pre.length === 0) return -1;
  for (let i = 0; i < Math.min(va.pre.length, vb.pre.length); i++) {
    const leftNumeric = /^\d+$/.test(va.pre[i]);
    const rightNumeric = /^\d+$/.test(vb.pre[i]);
    if (leftNumeric && !rightNumeric) return -1;
    if (!leftNumeric && rightNumeric) return 1;
    let result: number;
    if (leftNumeric) {
      result = compareNumeric(va.pre[i], vb.pre[i]);
    } else {
      const leftNatural = /^(.+?)(\d+)$/.exec(va.pre[i]);
      const rightNatural = /^(.+?)(\d+)$/.exec(vb.pre[i]);
      result =
        leftNatural && rightNatural && leftNatural[1] === rightNatural[1]
          ? compareNumeric(
              leftNatural[2].replace(/^0+(?=\d)/, ""),
              rightNatural[2].replace(/^0+(?=\d)/, "")
            )
          : va.pre[i] === vb.pre[i]
            ? 0
            : va.pre[i] < vb.pre[i]
              ? -1
              : 1;
    }
    if (result !== 0) return result;
  }
  return va.pre.length === vb.pre.length ? 0 : va.pre.length < vb.pre.length ? -1 : 1;
}

/**
 * 是否应触发探针自升级：agent 开启 auto_update，且服务端配置了
 * LATEST_AGENT_VERSION，且客户端上报的版本语义化低于它。
 */
export function shouldTriggerAgentUpdate(
  autoUpdate: boolean,
  latestVersion: unknown,
  agentVersion: string | null | undefined
): boolean {
  if (!autoUpdate) return false;
  if (typeof latestVersion !== "string" || latestVersion.trim() === "") {
    return false;
  }
  if (typeof agentVersion !== "string" || agentVersion.trim() === "") {
    return false;
  }
  return compareSemver(agentVersion, latestVersion) === -1;
}
