import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDb } from "../../../config/db";
import type { Bindings } from "../../../models/db";
import { createMigratedSqliteD1 } from "../../../test-utils/migrated-d1";
import type { SqliteD1 } from "../../../test-utils/sqlite-d1";
import { authenticateAgentToken, digestAgentToken } from "./D1AgentCredentialStore";
import { DrizzleAgentRepository } from "./DrizzleAgentRepository";

const NOW_MS = Date.UTC(2026, 9, 2, 12);
const NOW = new Date(NOW_MS).toISOString();
const TOKEN = "xga_test_fixture";
let sqlite: SqliteD1;
let env: Bindings;
let digest: string;

function changes() {
  return Number(sqlite.raw.prepare("SELECT total_changes() AS n").get()!.n);
}

function usage() {
  return sqlite.raw.prepare("SELECT last_used_at, updated_at FROM agent_credentials WHERE id = 1").get();
}

function setLastUsedAt(value: string | null) {
  sqlite.raw.prepare("UPDATE agent_credentials SET last_used_at = ?, updated_at = ? WHERE id = 1")
    .run(value, "2026-10-01T00:00:00.000Z");
}

beforeEach(async () => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW_MS);
  sqlite = createMigratedSqliteD1();
  env = { DB: sqlite.DB, AGENT_TOKEN_PEPPER: "test-pepper-with-at-least-32-characters" } as unknown as Bindings;
  digest = await digestAgentToken(env, TOKEN);
  sqlite.raw.exec(`
    INSERT INTO agents (id, name, token, created_at, updated_at)
    VALUES (1, 'test', 'legacy-fixture', '2026-10-01', '2026-10-01');
    INSERT INTO agent_nodes
      (id, name, collect_interval_ms, report_interval_ms, created_at_ms, updated_at_ms)
    VALUES (1, 'test', 1000, 60000, 0, 0);
    INSERT INTO agent_runtime (agent_id, status, created_at_ms, updated_at_ms)
    VALUES (1, 'active', 0, 0);
  `);
  sqlite.raw.prepare(`
    INSERT INTO agent_credentials
      (id, agent_id, token_digest, token_hint, last_used_at, created_at, updated_at)
    VALUES (1, 1, ?, 'fixture', ?, ?, ?)
  `).run(digest, NOW, NOW, NOW);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  sqlite.close();
});

describe.each(["repository", "token store"] as const)("credential usage: %s", (path) => {
  function authenticate() {
    return path === "repository"
      ? new DrizzleAgentRepository(env, createDb(env)).authenticateCredential({ token: TOKEN, digest, now: NOW })
      : authenticateAgentToken(env, TOKEN);
  }

  it("五分钟内认证正常且不发 UPDATE", async () => {
    setLastUsedAt(new Date(NOW_MS - 299999).toISOString());
    const before = usage();
    const count = changes();
    const prepare = vi.spyOn(sqlite.DB, "prepare");
    for (let i = 0; i < 5; i++) expect(await authenticate()).toMatchObject({ id: 1 });
    expect(usage()).toEqual(before);
    expect(changes()).toBe(count);
    expect(prepare.mock.calls.some(([sql]) => /^update\s+["`]?agent_credentials/i.test(sql.trim()))).toBe(false);
  });

  it.each([null, "not-a-timestamp", new Date(NOW_MS - 300000).toISOString()])(
    "首次使用、损坏时间或达到五分钟时更新一次：%s", async (lastUsedAt) => {
      setLastUsedAt(lastUsedAt);
      const count = changes();
      expect(await authenticate()).toMatchObject({ id: 1 });
      expect(usage()).toEqual({ last_used_at: NOW, updated_at: NOW });
      expect(changes() - count).toBe(1);
      expect(await authenticate()).toMatchObject({ id: 1 });
      expect(changes() - count).toBe(1);
    }
  );

  it.each([null, new Date(NOW_MS - 300000).toISOString()])(
    "重叠认证通过 compare-and-set 合并为一次写入：%s", async (lastUsedAt) => {
      setLastUsedAt(lastUsedAt);
      const count = changes();
      const agents = await Promise.all([authenticate(), authenticate(), authenticate()]);
      expect(agents.every((agent) => agent?.id === 1)).toBe(true);
      expect(changes() - count).toBe(1);
    }
  );

  it("时钟回退不把使用时间倒写", async () => {
    setLastUsedAt(new Date(NOW_MS + 60000).toISOString());
    const before = usage();
    const count = changes();
    expect(await authenticate()).toMatchObject({ id: 1 });
    expect(usage()).toEqual(before);
    expect(changes()).toBe(count);
  });

  it.each(["revoked", "deleted", "missing"] as const)(
    "%s 凭据/节点立即失效，不更新使用时间", async (state) => {
      setLastUsedAt(null);
      if (state === "revoked") sqlite.raw.exec("UPDATE agent_credentials SET revoked_at = '2026-10-01' WHERE id = 1");
      if (state === "deleted") sqlite.raw.exec("UPDATE agent_nodes SET deleted_at_ms = 1 WHERE id = 1");
      if (state === "missing") digest = "missing-digest";
      const before = usage();
      const count = changes();
      const result = state === "missing" && path === "token store"
        ? await authenticateAgentToken(env, "xga_missing_fixture")
        : await authenticate();
      expect(result).toBeNull();
      expect(usage()).toEqual(before);
      expect(changes()).toBe(count);
    }
  );
});
