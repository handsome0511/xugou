import { describe, expect, it } from "vitest";

import { MAX_REPORT_SAMPLES } from "../../../utils/agentConfig";
import {
  agentLiveFrameSchema,
  liveFrameToBroadcastUpdate,
} from "./AgentLiveProtocol";

const BASE_MS = Date.parse("2026-08-27T00:00:00.000Z");

function payload(second: number) {
  return {
    collected_at: new Date(BASE_MS + second * 1000).toISOString(),
    cpu: { usage: second, cores: 2, model_name: "test" },
    memory: { total: 100, used: 50, free: 50, usage_rate: 50 },
    load: { load1: 0.1, load5: 0.2, load15: 0.3 },
    network: [
      { interface: "eth0", bytes_recv: second * 1000, bytes_sent: second * 500 },
    ],
    network_rx_speed: 1000,
    network_tx_speed: 500,
  };
}

function batchFrame(count: number) {
  return {
    type: "metric_batch",
    protocol_version: 2,
    sequence: 7,
    samples: Array.from({ length: count }, (_unused, index) => payload(index)),
  };
}

describe("agentLiveFrameSchema", () => {
  it("接受 v2 攒批帧", () => {
    const parsed = agentLiveFrameSchema.safeParse(batchFrame(12));
    expect(parsed.success).toBe(true);
  });

  // v1.4.2 起不再接受 v1 单点帧（升级窗口已关闭）。留这条是为了让"重新放开
  // v1"成为一个显式决定，而不是某次改 schema 时悄悄溜回来。
  it("拒绝已下线的 v1 单点帧", () => {
    const parsed = agentLiveFrameSchema.safeParse({
      type: "metric",
      protocol_version: 1,
      sequence: 3,
      ...payload(0),
    });
    expect(parsed.success).toBe(false);
  });

  it("拒绝空批次——空帧只会白白消耗一次请求", () => {
    expect(agentLiveFrameSchema.safeParse(batchFrame(0)).success).toBe(false);
  });

  it("批次样本数上限与 MAX_REPORT_SAMPLES 对齐", () => {
    expect(
      agentLiveFrameSchema.safeParse(batchFrame(MAX_REPORT_SAMPLES)).success
    ).toBe(true);
    expect(
      agentLiveFrameSchema.safeParse(batchFrame(MAX_REPORT_SAMPLES + 1)).success
    ).toBe(false);
  });

  it("拒绝版本与类型不匹配的帧", () => {
    expect(
      agentLiveFrameSchema.safeParse({ ...batchFrame(1), protocol_version: 1 })
        .success
    ).toBe(false);
    expect(
      agentLiveFrameSchema.safeParse({ ...batchFrame(1), type: "metric" }).success
    ).toBe(false);
  });

  it("拒绝 128 个以上的 ping 目标", () => {
    const ping = Object.fromEntries(
      Array.from({ length: 129 }, (_unused, index) => [
        `t${index}`,
        { target: "example.com:443", latency_ms: 1, loss: false },
      ])
    );
    const frame = batchFrame(1);
    frame.samples[0] = { ...payload(0), ping } as (typeof frame.samples)[number];
    expect(agentLiveFrameSchema.safeParse(frame).success).toBe(false);
  });
});

describe("liveFrameToBroadcastUpdate", () => {
  it("批次里的每个采样点都各自成为一个广播样本，且保持采集顺序", () => {
    const parsed = agentLiveFrameSchema.parse(batchFrame(3));
    const update = liveFrameToBroadcastUpdate(42, parsed);

    expect(update.agentId).toBe(42);
    expect(update.samples).toHaveLength(3);
    expect(update.samples.map((sample) => sample.data.cpu_usage)).toEqual([0, 1, 2]);
    expect(update.samples.map((sample) => sample.ts)).toEqual([
      Date.parse(payload(0).collected_at),
      Date.parse(payload(1).collected_at),
      Date.parse(payload(2).collected_at),
    ]);
  });

  // 状态字段取批内最后一个采样点：攒批之后「最新」不再等于「唯一」。
  it("lastSeenAt 取批内最后一个采样点", () => {
    const parsed = agentLiveFrameSchema.parse(batchFrame(3));
    const update = liveFrameToBroadcastUpdate(42, parsed);

    expect(update.status).toBe("active");
    expect(update.lastSeenAt).toBe(payload(2).collected_at);
    expect(update.changedAt).toBe(payload(2).collected_at);
  });

  it("匿名状态页投影按样本各自生成", () => {
    const parsed = agentLiveFrameSchema.parse(batchFrame(2));
    const update = liveFrameToBroadcastUpdate(42, parsed);

    for (const sample of update.samples) {
      expect(sample.publicData).toBeDefined();
      expect(Object.keys(sample.publicData ?? {}).length).toBeGreaterThan(0);
    }
  });
});
