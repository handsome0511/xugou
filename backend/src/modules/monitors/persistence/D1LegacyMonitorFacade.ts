import type { Bindings } from "../../../models/db";
import type { MonitorMutation } from "../domain/models";
import { createMonitorUseCases } from "../composition";

const SINGLE_HISTORY_LIMIT = 1440;
const ALL_HISTORY_LIMIT = 10_000;
const DEFAULT_DAILY_STATS_DAYS = 90;
const MAX_DAILY_STATS_DAYS = 366;

function parseHeaders(value: unknown): Record<string, string> {
  if (typeof value === "string") {
    try {
      return parseHeaders(JSON.parse(value));
    } catch {
      return {};
    }
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>).map(([key, item]) => [
      key,
      String(item),
    ])
  );
}

export function toMonitorMutation(input: {
  name: string;
  url: string;
  method: string;
  interval: number;
  timeout: number;
  expected_status: number;
  headers?: unknown;
  body?: string | null;
  active?: boolean;
}): MonitorMutation {
  return {
    name: input.name,
    url: input.url,
    method: input.method.toUpperCase(),
    interval_seconds: input.interval,
    timeout_ms: input.timeout * 1000,
    expected_status: input.expected_status,
    headers: parseHeaders(input.headers),
    body: input.body ?? null,
    active: input.active,
  };
}

function historyCutoff() {
  return new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
}

function orderDailyStatsAscending(rows: Record<string, unknown>[]) {
  return rows.sort(
    (left, right) =>
      String(left.date ?? "").localeCompare(String(right.date ?? "")) ||
      Number(left.monitor_id ?? 0) - Number(right.monitor_id ?? 0)
  );
}

export async function queryMonitorHistory(env: Bindings, monitorId?: number) {
  const filter = monitorId === undefined ? "" : "AND monitor_id = ?";
  const bindings = monitorId === undefined ? [historyCutoff()] : [historyCutoff(), monitorId];
  const limit = monitorId === undefined ? ALL_HISTORY_LIMIT : SINGLE_HISTORY_LIMIT;
  const samples = await env.DB.prepare(
    `SELECT job_id AS id, monitor_id, status, checked_at AS timestamp,
            response_time_ms AS response_time, status_code, error
     FROM monitor_check_samples
     WHERE checked_at >= ? ${filter}
     ORDER BY checked_at DESC LIMIT ?`
  )
    .bind(...bindings, limit)
    .all<Record<string, unknown>>();
  return samples.results.reverse();
}

export async function queryMonitorDailyStats(
  env: Bindings,
  monitorId?: number,
  days = DEFAULT_DAILY_STATS_DAYS
) {
  const boundedDays = Math.max(1, Math.min(MAX_DAILY_STATS_DAYS, days));
  const cutoff = new Date(
    Date.now() - (boundedDays - 1) * 24 * 60 * 60 * 1000
  );
  cutoff.setUTCHours(0, 0, 0, 0);
  const cutoffIso = cutoff.toISOString();
  const limit = monitorId === undefined ? ALL_HISTORY_LIMIT : boundedDays;
  const filter = monitorId === undefined ? "" : "AND monitor_id = ?";
  const statement = env.DB.prepare(
    `SELECT id, monitor_id, substr(bucket_start, 1, 10) AS date,
            total_checks, up_checks, down_checks,
            response_time_avg AS avg_response_time,
            response_time_min AS min_response_time,
            response_time_max AS max_response_time,
            CASE WHEN total_checks > 0
                 THEN (CAST(up_checks AS REAL) / total_checks) * 100 ELSE 0 END
              AS availability,
            created_at
     FROM monitor_check_rollups
     WHERE bucket_size_seconds = 86400 AND bucket_start >= ? ${filter}
     ORDER BY bucket_start DESC, monitor_id ASC LIMIT ?`
  );
  const rows = (
    monitorId === undefined
      ? await statement.bind(cutoffIso, limit).all<Record<string, unknown>>()
      : await statement
          .bind(cutoffIso, monitorId, limit)
          .all<Record<string, unknown>>()
  ).results;
  return orderDailyStatsAscending(rows);
}

export async function updateLegacyMonitorOrder(env: Bindings, ids: number[]) {
  const uniqueIds = [...new Set(ids)];
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS count FROM monitor_definitions
     WHERE deleted_at_ms IS NULL
       AND id IN (SELECT CAST(value AS INTEGER) FROM json_each(?))`
  )
    .bind(JSON.stringify(uniqueIds))
    .first<{ count: number }>();
  if (Number(row?.count ?? 0) !== uniqueIds.length) return false;
  for (let offset = 0; offset < uniqueIds.length; offset += 25) {
    await env.DB.batch(
      uniqueIds.slice(offset, offset + 25).map((id, index) =>
        env.DB.prepare(
          `UPDATE monitor_definitions SET sort_order = ?, updated_at_ms = ?
           WHERE id = ? AND deleted_at_ms IS NULL`
        ).bind(offset + index, Date.now(), id)
      )
    );
  }
  return true;
}

export async function importLegacyMonitors(
  env: Bindings,
  items: Array<ReturnType<typeof toMonitorMutation> & { sort_order?: number }>
) {
  const candidateNames = [...new Set(items.map((item) => item.name))];
  const existingRows = await env.DB.prepare(
    `SELECT name FROM monitor_definitions
     WHERE deleted_at_ms IS NULL
       AND name IN (SELECT value FROM json_each(?))`
  )
    .bind(JSON.stringify(candidateNames))
    .all<{ name: string }>();
  const names = new Set(existingRows.results.map((row) => row.name));
  let created = 0;
  let skipped = 0;
  for (const item of items) {
    if (names.has(item.name)) {
      skipped += 1;
      continue;
    }
    try {
      const view = await createMonitorUseCases(env).create(item);
      if (Number.isInteger(item.sort_order)) {
        await env.DB.prepare(
          `UPDATE monitor_definitions
           SET sort_order = ?, updated_at_ms = ? WHERE id = ?`
        )
          .bind(item.sort_order, Date.now(), view.id)
          .run();
      }
      names.add(item.name);
      created += 1;
    } catch (error) {
      skipped += 1;
    }
  }
  return { created, skipped };
}
