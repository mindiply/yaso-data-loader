type NotNull<T> = T extends null
  ? never
  : T extends undefined
    ? undefined
    : T extends Date
      ? Date
      : T extends Buffer
        ? Buffer
        : T extends Set<infer V>
          ? Set<NotNull<V>>
          : T extends Map<infer K, infer V>
            ? Map<K, NotNull<V>>
            : T extends Array<infer V>
              ? Array<NotNull<V>>
              : T extends Record<any, any>
                ? {
                  [P in keyof T]: NotNull<T[P]>;
                }
                : Exclude<T, null>;

export function extractNonNullable<T>(obj: T): NotNull<T> {
  if (obj === null) {
    throw new Error('Unexpected null value');
  }
  if (obj === undefined) {
    return undefined as NotNull<T>;
  }
  if (
    typeof obj === 'number' ||
    typeof obj === 'boolean' ||
    typeof obj === 'string' ||
    typeof obj === 'symbol' ||
    obj instanceof Date ||
    obj instanceof Buffer
  ) {
    return obj as NotNull<T>;
  }
  if (obj instanceof Set) {
    return new Set(
      Array.from(obj.values()).map(extractNonNullable)
    ) as NotNull<T>;
  }
  if (obj instanceof Map) {
    return new Map(
      Array.from(obj.entries()).map(([k, v]) => [k, extractNonNullable(v)])
    ) as NotNull<T>;
  }
  if (Array.isArray(obj)) {
    return obj.map(extractNonNullable) as NotNull<T>;
  }
  if (typeof obj !== 'object') {
    return Object.keys(obj).reduce((acc, key) => {
      // @ts-expect-error trick to make TS happy
      acc[key] = extractNonNullable(obj[key]);
      return acc;
    }, {} as NotNull<T>) as NotNull<T>;
  }
  return obj as NotNull<T>;
}
