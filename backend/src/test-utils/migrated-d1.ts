import { readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { createSqliteD1 } from "./sqlite-d1";

const migrationsDirectory = resolve(__dirname, "../../drizzle");

export function readMigration(name: string) {
  return readFileSync(resolve(migrationsDirectory, name), "utf8");
}

/** 用真实、按版本排序的迁移验证持久层，避免手写 schema 漏掉生产索引。 */
export function createMigratedSqliteD1(through?: string) {
  const schema = readdirSync(migrationsDirectory)
    .filter((name) => name.endsWith(".sql") && (!through || name <= through))
    .sort()
    .map(readMigration)
    .join("\n");
  return createSqliteD1(schema);
}
