import type { MutationOptions, StoreOptions } from "./types.ts";
import { isObject } from "../utils.ts";
import { queryOptionsBrand } from "./symbols.ts";

type SupportedOptions = StoreOptions<any> | MutationOptions;

/**
 * Mark query or mutation options for use in the first argument position when
 * the operation has no input.
 *
 * The returned object is a frozen shallow copy with the same option fields and
 * a private Aeolia brand. Prefer this helper when an options object could be
 * mistaken for operation input, especially when the options object is empty.
 * For an inputless query or mutation, a second options argument may override
 * matching fields in the branded object. The helper does not validate
 * operation-specific option values; the graph validates them when called.
 *
 * @param options - Store options for a query or invocation options for a mutation.
 * @returns A shallow-frozen, readonly copy with a private brand used by graph
 * members.
 * @throws A `TypeError` when `options` is not a non-array object.
 */
export function queryOptions<T extends SupportedOptions>(
  options: T,
): Readonly<T> & {
  readonly [queryOptionsBrand]: true;
} {
  if (!isObject(options) || typeof options === "function" || Array.isArray(options))
    throw new TypeError("queryOptions expects an options object.");
  return Object.freeze({ ...options, [queryOptionsBrand]: true }) as Readonly<T> & {
    readonly [queryOptionsBrand]: true;
  };
}

export function hasQueryOptionsBrand(value: unknown): value is {
  readonly [queryOptionsBrand]: true;
} {
  return (
    isObject(value) &&
    typeof value !== "function" &&
    Object.hasOwn(value, queryOptionsBrand) &&
    (value as { readonly [queryOptionsBrand]?: unknown })[queryOptionsBrand] === true
  );
}
