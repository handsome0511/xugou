import { describe, expect, it } from "vitest";

import { streamCommandFor } from "./StreamGate";

describe("streamCommandFor", () => {
  it("第一个订阅者到达时开启推流", () => {
    expect(streamCommandFor(0, 1)).toBe(true);
  });

  it("最后一个订阅者离开时暂停推流", () => {
    expect(streamCommandFor(1, 0)).toBe(false);
  });

  // 只在边沿下发是重点：每来一个订阅者就发一条指令的话，
  // 指令本身就变成了新的请求源，等于把省下来的又花回去。
  it("订阅者增减但没跨过 0 时不下发指令", () => {
    expect(streamCommandFor(1, 2)).toBeNull();
    expect(streamCommandFor(3, 2)).toBeNull();
    expect(streamCommandFor(2, 2)).toBeNull();
  });

  it("本来就没人看，走掉一个不存在的订阅者也不下发", () => {
    expect(streamCommandFor(0, 0)).toBeNull();
  });

  // 计数出现负数说明上游算错了，此时按「没人看」处理，
  // 不能因为 -1 !== 0 就误判成有人在看而一直推流。
  it("计数为负时按无人订阅处理", () => {
    expect(streamCommandFor(-1, 0)).toBeNull();
    expect(streamCommandFor(1, -1)).toBe(false);
    expect(streamCommandFor(-1, 2)).toBe(true);
  });
});
