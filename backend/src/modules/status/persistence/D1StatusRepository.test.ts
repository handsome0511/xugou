import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getTableConfig } from "drizzle-orm/sqlite-core";
import { monitorCheckRollups } from "../../../db/schema";
import {
  createMigratedSqliteD1,
  readMigration,
} from "../../../test-utils/migrated-d1";
import type { SqliteD1 } from "../../../test-utils/sqlite-d1";
import type { Bindings } from "../../../models/db";
import { queryMonitorDailyStats } from "../../monitors/persistence/D1LegacyMonitorFacade";
import { D1StatusRepository } from "./D1StatusRepository";

const NOW_MS = Date.UTC(2026, 9, 2, 12);
const MIGRATION = "0053_sticky_mauler.sql";
const INDEX = "monitor_check_rollups_size_monitor_bucket_idx";
let sqlite: SqliteD1;
let env: Bindings;

function queryPlan(sql: string, ...params: (string | number)[]) {
  const rows = sqlite.raw.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params) as {
    detail: string;
  }[];
  return rows.map((row) => row.detail).join(" / ");
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW_MS);
  sqlite = createMigratedSqliteD1("0052_giant_lady_deathstrike.sql");
  env = { DB: sqlite.DB } as unknown as Bindings;
  sqlite.raw.exec(`
    INSERT INTO monitors
      (id, name, url, method, interval, timeout, expected_status, headers,
       active, created_at, updated_at)
    VALUES (1, 'selected', 'https://example.com', 'GET', 300, 30, 200, '{}',
            1, '2026-07-01', '2026-07-01'),
           (2, 'unselected', 'https://example.com', 'GET', 300, 30, 200, '{}',
            1, '2026-07-01', '2026-07-01');
    INSERT INTO monitor_definitions
      (id, name, url, method, interval_ms, timeout_ms, expected_status,
       active, created_at_ms, updated_at_ms)
    SELECT id, name, url, method, 300000, 30000, 200, 1, 0, 0 FROM monitors;
    INSERT INTO monitor_runtime
      (monitor_id, status, response_time_ms, created_at_ms, updated_at_ms)
    SELECT id, 'up', 100, 0, 0 FROM monitors;
    INSERT INTO status_pages
      (id, singleton_key, title, theme, created_at_ms, updated_at_ms)
    VALUES (1, 1, 'status', 'mono', 0, 0);
    INSERT INTO status_components VALUES (1, 'monitor', 1, 0, 0, 0);
    INSERT INTO monitor_check_rollups
      (monitor_id, bucket_start, bucket_size_seconds, total_checks, up_checks,
       down_checks, response_time_avg, created_at, updated_at)
    VALUES (1, '2026-09-30T00:00:00.000Z', 86400, 10, 9, 1, 100, '2026-10-01', '2026-10-01'),
           (1, '2026-10-01T00:00:00.000Z', 86400, 10, 10, 0, 120, '2026-10-02', '2026-10-02'),
           (1, '2026-06-01T00:00:00.000Z', 86400, 10, 10, 0, 120, '2026-06-02', '2026-06-02'),
           (2, '2026-10-01T00:00:00.000Z', 86400, 10, 0, 10, 999, '2026-10-02', '2026-10-02');
    -- 与线上相同的混合粒度分布：大量五分钟行，日统计只取极少数行。
    WITH RECURSIVE ticks(n) AS (
      SELECT 0 UNION ALL SELECT n + 1 FROM ticks WHERE n < 9999
    )
    INSERT INTO monitor_check_rollups
      (monitor_id, bucket_start, bucket_size_seconds, total_checks,
       up_checks, created_at, updated_at)
    SELECT 1, strftime('%Y-%m-%dT%H:%M:%fZ', '2026-08-01', '+' || (n * 300) || ' seconds'),
           300, 1, 1, '2026-10-02', '2026-10-02' FROM ticks;
  `);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  sqlite.close();
});

describe("monitor rollup index migration", () => {
  it("公开日统计只扫描指定粒度，迁移保留数据和唯一键，五分钟查询仍走索引", async () => {
    const prepare = vi.spyOn(sqlite.DB, "prepare");
    const repository = new D1StatusRepository(env);
    const before = await repository.buildPublicData();
    const dailySql = prepare.mock.calls.find(
      ([sql]) => sql.includes("FROM monitor_check_rollups") && sql.includes("bucket_size_seconds = 86400")
    )![0];
    const fiveMinuteSql = prepare.mock.calls.find(
      ([sql]) => sql.includes("FROM monitor_check_rollups") && sql.includes("bucket_size_seconds = 300")
    )![0];
    expect(queryPlan(dailySql, 1, "2026-07-04")).not.toContain(INDEX);
    const rowsBefore = sqlite.raw.prepare("SELECT COUNT(*) AS n FROM monitor_check_rollups").get();

    sqlite.raw.exec(readMigration(MIGRATION));

    expect(await repository.buildPublicData()).toEqual(before);
    expect(before.monitors).toHaveLength(1);
    expect(before.monitors[0].dailyStats).toMatchObject([
      { date: "2026-09-30", availability: 90 },
      { date: "2026-10-01", availability: 100 },
    ]);
    expect(sqlite.raw.prepare("SELECT COUNT(*) AS n FROM monitor_check_rollups").get()).toEqual(rowsBefore);
    for (const sql of [dailySql, fiveMinuteSql]) {
      const plan = queryPlan(sql, 1, "2026-07-04");
      expect(plan).toContain(`USING INDEX ${INDEX}`);
      expect(plan).toContain("bucket_size_seconds=? AND monitor_id=? AND bucket_start>?");
      expect(plan).not.toContain("SCAN monitor_check_rollups");
    }
    const indexes = sqlite.raw.prepare("PRAGMA index_list(monitor_check_rollups)").all() as {
      name: string;
      unique: number;
    }[];
    expect(indexes.map((row) => row.name).sort()).toEqual(
      getTableConfig(monitorCheckRollups).indexes.map((index) => index.config.name).sort()
    );
    expect(indexes.find((row) => row.name.endsWith("unique_idx"))?.unique).toBe(1);
    expect(() => sqlite.raw.exec(`
      INSERT INTO monitor_check_rollups
        (monitor_id, bucket_start, bucket_size_seconds, created_at, updated_at)
      VALUES (1, '2026-10-01T00:00:00.000Z', 86400, '2026-10-02', '2026-10-02')
    `)).toThrow(/UNIQUE/);
  });

  it("管理端单个和全部 Monitor 的日统计均按粒度索引查询，结果不变", async () => {
    const singleBefore = await queryMonitorDailyStats(env, 1);
    const allBefore = await queryMonitorDailyStats(env);
    sqlite.raw.exec(readMigration(MIGRATION));
    const prepare = vi.spyOn(sqlite.DB, "prepare");
    expect(await queryMonitorDailyStats(env, 1)).toEqual(singleBefore);
    expect(await queryMonitorDailyStats(env)).toEqual(allBefore);
    const singleSql = prepare.mock.calls[0][0];
    const allSql = prepare.mock.calls[1][0];
    expect(queryPlan(singleSql, "2026-07-05", 1, 90)).toContain(
      "bucket_size_seconds=? AND monitor_id=? AND bucket_start>?"
    );
    expect(queryPlan(allSql, "2026-07-05", 10000)).toContain(`USING INDEX ${INDEX}`);
    expect(singleBefore).toHaveLength(2);
    expect(allBefore).toHaveLength(3);
  });
});
