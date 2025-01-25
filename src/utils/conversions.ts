export function toDateTime(inpDate?: Date | string | null): Date {
  return inpDate
    ? typeof inpDate === 'string'
      ? new Date(inpDate)
      : inpDate
    : new Date();
}

export interface EscapedDate {
  __typename: 'EscapedDate';
  isoString: string;
}

interface EscapedSet<V = unknown> {
  __typename: 'EscapedSet';
  values: V[];
}

interface EscapedMap<K = unknown, V = unknown> {
  __typename: 'EscapedMap';
  values: Array<[K, V]>;
}

interface EscapedBuffer {
  __typename: 'EscapedBuffer';
  base64Str: string;
}

export type EscapedObject<T> = T extends Date
  ? EscapedDate
  : T extends Buffer
    ? EscapedBuffer
    : T extends Set<infer V>
      ? EscapedSet<V>
      : T extends Map<infer K, infer V>
        ? EscapedMap<K, V>
        : T extends Record<string, any>
          ? {
              [K in keyof T]: EscapedObject<T[K]>;
            }
          : T extends Array<infer E>
            ? Array<EscapedObject<E>>
            : T;

export type UnescapedObject<T> = T extends EscapedDate
  ? Date
  : T extends EscapedBuffer
    ? Buffer
    : T extends EscapedSet<infer V>
      ? Set<V>
      : T extends EscapedMap<infer K, infer V>
        ? Map<K, V>
        : T extends Record<string, any>
          ? {
              [K in keyof T]: UnescapedObject<T[K]>;
            }
          : T extends Array<infer E>
            ? Array<UnescapedObject<E>>
            : T;

export function escapeForJson<T>(val: T): EscapedObject<T> {
  if (val && val instanceof Date) {
    const escapedDate: EscapedDate = {
      __typename: 'EscapedDate',
      isoString: val.toISOString()
    };
    return escapedDate as unknown as EscapedObject<T>;
  } else if (val && val instanceof Buffer) {
    const escapedBuffer: EscapedBuffer = {
      __typename: 'EscapedBuffer',
      base64Str: val.toString('base64')
    };
    return escapedBuffer as unknown as EscapedObject<T>;
  } else if (val && val instanceof Set) {
    return {
      __typename: 'EscapedSet',
      values: Array.from(val.values()).map(value => escapeForJson(value))
    } as EscapedObject<T>;
  } else if (val && val instanceof Map) {
    return {
      __typename: 'EscapedMap',
      values: Array.from(val.entries()).map(([key, value]) => [
        escapeForJson(key),
        escapeForJson(value)
      ])
    } as EscapedObject<T>;
  } else if (Array.isArray(val)) {
    return val.map(el => escapeForJson(el)) as EscapedObject<T>;
  } else if (val instanceof Object && isPlainObj(val)) {
    const mappedVal = {...val};
    for (const key in mappedVal) {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error
      mappedVal[key] = escapeForJson(mappedVal[key]);
    }
    return mappedVal as EscapedObject<T>;
  }
  return val as EscapedObject<T>;
}

export function deEscapeFromJson<T>(val: T): UnescapedObject<T> {
  if (val && val instanceof Date) {
    return val as UnescapedObject<T>;
  } else if (val && val instanceof Buffer) {
    return val as UnescapedObject<T>;
  } else if (val && val instanceof Set) {
    return new Set(
      Array.from(val.values()).map(val => deEscapeFromJson(val))
    ) as UnescapedObject<T>;
  } else if (val && val instanceof Map) {
    return new Map(
      Array.from(val.entries()).map(([key, value]) => [
        deEscapeFromJson(key),
        deEscapeFromJson(value)
      ])
    ) as UnescapedObject<T>;
  } else if (isEscapedDate(val)) {
    return new Date(val.isoString) as unknown as UnescapedObject<T>;
  } else if (isEscapedSet(val)) {
    return new Set(
      val.values.map(value => deEscapeFromJson(value))
    ) as UnescapedObject<T>;
  } else if (Array.isArray(val)) {
    return val.map(el => deEscapeFromJson(el)) as UnescapedObject<T>;
  } else if (isEscapedMap(val)) {
    return new Map(
      val.values.map(([key, value]) => [
        deEscapeFromJson(key),
        deEscapeFromJson(value)
      ])
    ) as UnescapedObject<T>;
  } else if (isEscapedBuffer(val)) {
    return Buffer.from(
      (val as EscapedBuffer).base64Str,
      'base64'
    ) as UnescapedObject<T>;
  } else if (val && val instanceof Object) {
    if (isPlainObj(val)) {
      const unescapedVal = Object.assign({}, val) as typeof val;
      for (const key of Object.keys(unescapedVal)) {
        if (
          (key === 'when' ||
            key === 'startDate' ||
            key === 'endDate' ||
            key === 'dueDate' ||
            key === 'completedOn' ||
            key === 'baselineStartDate' ||
            key === 'baselineEndDate' ||
            key === 'referenceStartDate') &&
          unescapedVal[key as keyof typeof val] &&
          typeof unescapedVal[key as keyof typeof val] === 'string'
        ) {
          try {
            // @ts-expect-error complicated change in type
            unescapedVal[key] = new Date(unescapedVal[key] as string);
          } catch (err) {
            // do nothing, leave as string
          }
        } else {
          // @ts-expect-error complicated change in type
          unescapedVal[key as keyof typeof val] = deEscapeFromJson(
            unescapedVal[key as keyof typeof val]
          );
        }
      }
      return unescapedVal as unknown as UnescapedObject<T>;
    }
  }
  return val as UnescapedObject<T>;
}

function isEscapedSet(val: unknown): val is EscapedSet {
  if (
    val &&
    val instanceof Object &&
    (val as EscapedSet).__typename === 'EscapedSet' &&
    Array.isArray((val as EscapedSet).values)
  ) {
    return true;
  } else {
    return false;
  }
}

function isEscapedBuffer(val: unknown): val is EscapedBuffer {
  if (
    val &&
    val instanceof Object &&
    (val as EscapedBuffer).__typename === 'EscapedBuffer' &&
    typeof (val as EscapedBuffer).base64Str === 'string'
  ) {
    return true;
  } else {
    return false;
  }
}

function isEscapedMap(val: unknown): val is EscapedMap {
  if (
    val &&
    val instanceof Object &&
    (val as EscapedMap).__typename === 'EscapedMap' &&
    Array.isArray((val as EscapedMap).values) &&
    (val as EscapedMap).values.every(
      (item: unknown) => Array.isArray(item) && item.length === 2
    )
  ) {
    return true;
  } else {
    return false;
  }
}

function isEscapedDate(val: unknown): val is EscapedDate {
  if (
    val &&
    val instanceof Object &&
    (val as EscapedDate).__typename === 'EscapedDate' &&
    typeof (val as EscapedDate).isoString === 'string'
  ) {
    try {
      const dateVal = Date.parse((val as EscapedDate).isoString);
      return !Number.isNaN(dateVal);
    } catch (_) {
      // rely on outer return
    }
  }
  return false;
}

function isPlainObj(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object') {
    return false;
  }

  if (value === undefined || value === null) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);

  if (prototype === null || prototype === Object.getPrototypeOf({})) {
    return true;
  }

  return value.constructor === Object;
}
