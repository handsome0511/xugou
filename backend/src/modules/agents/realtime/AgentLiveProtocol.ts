import { z } from "zod";

import type {
  BroadcastMetricData,
  BroadcastSample,
  BroadcastUpdate,
} from "../../../models/broadcast";
import { MAX_REPORT_SAMPLES } from "../../../utils/agentConfig";
import { projectPublicRealtimeMetric } from "../../status/domain/public-contract";

const boundedMetric = z.number().finite().min(0).max(Number.MAX_SAFE_INTEGER);
const percentMetric = z.number().finite().min(0).max(100);
const diskMetric = z
  .object({
    device: z.string().max(512).optional(),
    mount_point: z.string().max(512).optional(),
    total: boundedMetric.optional(),
    used: boundedMetric.optional(),
    free: boundedMetric.optional(),
    usage_rate: percentMetric.optional(),
    fs_type: z.string().max(128).optional(),
  })
  .strict();
const networkMetric = z
  .object({
    interface: z.string().max(512).optional(),
    bytes_sent: boundedMetric.optional(),
    bytes_recv: boundedMetric.optional(),
    packets_sent: boundedMetric.optional(),
    packets_recv: boundedMetric.optional(),
  })
  .strict();
const pingMetric = z
  .object({
    target: z.string().max(512).optional(),
    latency_ms: z.number().finite().min(-1).max(Number.MAX_SAFE_INTEGER).optional(),
    loss: z.boolean().optional(),
  })
  .strict();

/** 单个采样点。v1 把它摊平进帧本体，v2 把 N 个装进一帧的 samples。 */
const liveMetricPayloadShape = {
  collected_at: z.string().datetime({ offset: true }),
  cpu: z
    .object({
      usage: percentMetric,
      cores: z.number().int().positive().max(4096),
      model_name: z.string().max(512),
    })
    .strict(),
  memory: z
    .object({
      total: boundedMetric,
      used: boundedMetric,
      free: boundedMetric,
      usage_rate: percentMetric,
    })
    .strict(),
  load: z
    .object({
      load1: boundedMetric,
      load5: boundedMetric,
      load15: boundedMetric,
    })
    .strict(),
  disks: z.array(diskMetric).max(128).optional(),
  network: z.array(networkMetric).max(128).optional(),
  swap: z
    .object({
      total: boundedMetric,
      used: boundedMetric,
      usage_rate: percentMetric,
    })
    .strict()
    .nullable()
    .optional(),
  process_count: z.number().int().nonnegative().max(10_000_000).optional(),
  tcp_connections: z.number().int().nonnegative().max(10_000_000).optional(),
  udp_connections: z.number().int().nonnegative().max(10_000_000).optional(),
  // 上限挂在字段自身而不是帧的 superRefine 上：帧 schema 必须保持 ZodObject，
  // 否则进不了下面按 type 判别的 discriminatedUnion。
  ping: z
    .record(pingMetric)
    .refine((value) => Object.keys(value).length <= 128, {
      message: "Ping 指标最多允许 128 个目标",
    })
    .optional(),
  ipv4_reachable: z.boolean().nullable().optional(),
  ipv6_reachable: z.boolean().nullable().optional(),
  network_rx_speed: boundedMetric.nullable(),
  network_tx_speed: boundedMetric.nullable(),
} as const;

const liveMetricPayloadSchema = z.object(liveMetricPayloadShape).strict();

const liveSequence = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);

/**
 * v1：一帧一个采样点。探针自升级有滞后，旧探针还会发这种帧，必须继续接受。
 */
export const agentLiveMetricFrameSchema = z
  .object({
    type: z.literal("metric"),
    protocol_version: z.literal(1),
    sequence: liveSequence,
    ...liveMetricPayloadShape,
  })
  .strict();

/**
 * v2：一帧承载一个攒批窗口内的全部采样点。
 *
 * Durable Object 的每条入站 WebSocket 消息都单独计一次 Worker 请求，秒级一帧
 * 时单台探针就是 86400 次/天。攒批只增加实时视图延迟，批内仍是逐秒样本。
 * samples 上限与 MAX_REPORT_SAMPLES 对齐，探针侧 maxBatchSamples 必须相同。
 */
export const agentLiveMetricBatchSchema = z
  .object({
    type: z.literal("metric_batch"),
    protocol_version: z.literal(2),
    sequence: liveSequence,
    samples: z.array(liveMetricPayloadSchema).min(1).max(MAX_REPORT_SAMPLES),
  })
  .strict();

export const agentLiveFrameSchema = z.discriminatedUnion("type", [
  agentLiveMetricFrameSchema,
  agentLiveMetricBatchSchema,
]);

export type AgentLiveMetricPayload = z.infer<typeof liveMetricPayloadSchema>;
export type AgentLiveMetricFrame = z.infer<typeof agentLiveMetricFrameSchema>;
export type AgentLiveMetricBatch = z.infer<typeof agentLiveMetricBatchSchema>;
export type AgentLiveFrame = z.infer<typeof agentLiveFrameSchema>;

function payloadToBroadcastSample(
  agentId: number,
  frame: AgentLiveMetricPayload
): BroadcastSample {
  const data: BroadcastMetricData & Record<string, unknown> = {
    agent_id: agentId,
    timestamp: frame.collected_at,
    cpu_usage: frame.cpu.usage,
    cpu_cores: frame.cpu.cores,
    cpu_model: frame.cpu.model_name,
    memory_total: frame.memory.total,
    memory_used: frame.memory.used,
    memory_free: frame.memory.free,
    memory_usage_rate: frame.memory.usage_rate,
    load_1: frame.load.load1,
    load_5: frame.load.load5,
    load_15: frame.load.load15,
    network_rx_speed: frame.network_rx_speed,
    network_tx_speed: frame.network_tx_speed,
  };
  if (frame.disks !== undefined) {
    data.disk_metrics = JSON.stringify(frame.disks);
  }
  if (frame.network !== undefined) {
    data.network_metrics = JSON.stringify(frame.network);
  }
  if (frame.swap !== undefined) {
    data.swap_total = frame.swap?.total ?? null;
    data.swap_used = frame.swap?.used ?? null;
  }
  if (frame.process_count !== undefined) {
    data.process_count = frame.process_count;
  }
  if (frame.tcp_connections !== undefined) {
    data.tcp_connections = frame.tcp_connections;
  }
  if (frame.udp_connections !== undefined) {
    data.udp_connections = frame.udp_connections;
  }
  if (frame.ping !== undefined) {
    data.ping_json = JSON.stringify(frame.ping);
  }
  if (frame.ipv4_reachable !== undefined) {
    data.ipv4_reachable =
      frame.ipv4_reachable === null ? null : frame.ipv4_reachable ? 1 : 0;
  }
  if (frame.ipv6_reachable !== undefined) {
    data.ipv6_reachable =
      frame.ipv6_reachable === null ? null : frame.ipv6_reachable ? 1 : 0;
  }
  const projected = projectPublicRealtimeMetric(data);
  const publicData = Object.fromEntries(
    Object.keys(data).flatMap((key) =>
      Object.hasOwn(projected, key) ? [[key, projected[key]]] : []
    )
  ) as BroadcastMetricData;
  const timestamp = Date.parse(frame.collected_at);
  return {
    ts: Number.isFinite(timestamp) ? timestamp : Date.now(),
    data,
    publicData,
  };
}

/**
 * 把 v1 单点帧或 v2 批次帧统一映射成一次广播更新。
 *
 * 批次内保持探针的采集顺序，状态字段取批内最后一个采样点——它才是"最新"。
 */
export function liveFrameToBroadcastUpdate(
  agentId: number,
  frame: AgentLiveFrame
): BroadcastUpdate {
  const payloads: AgentLiveMetricPayload[] =
    frame.type === "metric_batch" ? frame.samples : [frame];
  const latest = payloads[payloads.length - 1];
  return {
    agentId,
    status: "active",
    lastSeenAt: latest.collected_at,
    changedAt: latest.collected_at,
    samples: payloads.map((payload) => payloadToBroadcastSample(agentId, payload)),
  };
}
