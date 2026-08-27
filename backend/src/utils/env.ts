export function getEnvNumber(
  env: object | undefined,
  key: string,
  fallback: number,
  options: { min?: number; max?: number } = {}
) {
  const rawValue = env
    ? (env as Record<string, unknown>)[key]
    : undefined;
  if (
    rawValue === undefined ||
    rawValue === null ||
    (typeof rawValue === "string" && rawValue.trim() === "")
  ) {
    return fallback;
  }
  const value =
    typeof rawValue === "number" ? rawValue : Number(String(rawValue));

  if (!Number.isFinite(value)) {
    return fallback;
  }

  const rounded = Math.round(value);
  const min = options.min ?? Number.NEGATIVE_INFINITY;
  const max = options.max ?? Number.POSITIVE_INFINITY;

  return Math.min(Math.max(rounded, min), max);
}

