type PhysicalWorkKind =
  | "acquisition"
  | "query"
  | "mutation"
  | "mutation-phase"
  | "stream"
  | "stream-return"
  | "projection"
  | "projection-return"
  | "cleanup";

import { noteTransferChange, registerTransferWork, releaseTransferWork } from "./query-policy.ts";

const NativePromise = Promise;
const nativePromisePrototype = NativePromise.prototype;
const nativeResolve = NativePromise.resolve;
const nativeThen = nativePromisePrototype.then;
const nativeApply = Reflect.apply;
const readerGetterArguments: [] = [];
const defineOwnProperty = Object.defineProperty;
const deleteOwnProperty = Reflect.deleteProperty;
const ownDescriptor = Object.getOwnPropertyDescriptor;
const prototypeOf = Object.getPrototypeOf;
const speciesKey: typeof Symbol.species = Symbol.species;
const nativeConstructorDescriptor = ownDescriptor(nativePromisePrototype, "constructor");
const nativeSpeciesDescriptor = ownDescriptor(NativePromise, speciesKey);
const completionConstructor = { [speciesKey]: NativePromise };
const genuinePromises = new WeakSet<object>();
const promiseSetAdd = WeakSet.prototype.add;
const promiseSetHas = WeakSet.prototype.has;

function retainGenuinePromise<T>(promise: Promise<T>): Promise<T> {
  nativeApply(promiseSetAdd, genuinePromises, [promise]);
  return promise;
}

function descriptorField(descriptor: PropertyDescriptor, key: string): unknown {
  return ownDescriptor(descriptor, key)?.value;
}

function sameDescriptor(
  current: PropertyDescriptor | undefined,
  expected: PropertyDescriptor | undefined,
): boolean {
  if (current === undefined || expected === undefined) return current === expected;
  return (
    descriptorField(current, "value") === descriptorField(expected, "value") &&
    descriptorField(current, "writable") === descriptorField(expected, "writable") &&
    descriptorField(current, "get") === descriptorField(expected, "get") &&
    descriptorField(current, "set") === descriptorField(expected, "set") &&
    descriptorField(current, "enumerable") === descriptorField(expected, "enumerable") &&
    descriptorField(current, "configurable") === descriptorField(expected, "configurable")
  );
}

/** Normalize once with the captured constructor, yielding a genuine non-proxy Promise. */
export function normalizePhysicalPromise<T>(value: T | PromiseLike<T>): Promise<T> {
  return retainGenuinePromise(nativeApply(nativeResolve, NativePromise, [value]) as Promise<T>);
}

/** Construct a private native continuation without consulting mutable global constructors. */
export function createPhysicalPromise<T>(
  executor: (
    resolve: (value: T | PromiseLike<T>) => void,
    reject: (cause: unknown) => void,
  ) => void,
): Promise<T> {
  return retainGenuinePromise(new NativePromise<T>(executor));
}

/** Observe a private promise that could not be returned to its caller. */
export function observeUnreturnedPromise(promise: Promise<unknown>): void {
  const previous = ownDescriptor(promise, "constructor");
  defineOwnProperty(promise, "constructor", {
    configurable: true,
    value: completionConstructor,
  });
  try {
    void nativeApply(nativeThen, promise, [undefined, () => undefined]);
  } finally {
    if (previous === undefined) deleteOwnProperty(promise, "constructor");
    else defineOwnProperty(promise, "constructor", previous);
  }
}

/** Create completion before authored setup can expose or mutate the output Promise. */
export function createPhysicalCompletion<T>(settled?: () => void): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T | PromiseLike<T>) => void;
  readonly reject: (cause: unknown) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (cause: unknown) => void;
  const promise = createPhysicalPromise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  if (settled !== undefined) {
    // This promise is still private; restore its public shape before returning it.
    defineOwnProperty(promise, "constructor", {
      configurable: true,
      value: completionConstructor,
    });
    try {
      void nativeApply(nativeThen, promise, [settled, settled]);
    } finally {
      deleteOwnProperty(promise, "constructor");
    }
  }
  return { promise, resolve, reject };
}

/**
 * Check only an already-normalized genuine Promise, immediately before its
 * captured observer runs. Descriptors reject accessors without evaluating them.
 */
export function canReplacePhysicalCarrier(promise: Promise<unknown>, observer: unknown): boolean {
  if (
    !nativeApply(promiseSetHas, genuinePromises, [promise]) ||
    observer !== nativeThen ||
    prototypeOf(promise) !== nativePromisePrototype
  )
    return false;
  const constructor = ownDescriptor(promise, "constructor");
  if (constructor !== undefined && descriptorField(constructor, "value") !== NativePromise)
    return false;
  return (
    nativeConstructorDescriptor !== undefined &&
    descriptorField(nativeConstructorDescriptor, "value") === NativePromise &&
    sameDescriptor(
      ownDescriptor(nativePromisePrototype, "constructor"),
      nativeConstructorDescriptor,
    ) &&
    sameDescriptor(ownDescriptor(NativePromise, speciesKey), nativeSpeciesDescriptor)
  );
}

/** Carry provenance through one existing native continuation invocation. */
export function continuePhysicalPromise<T, R = unknown>(
  promise: Promise<T>,
  callbacks: (certified: boolean) => {
    readonly fulfilled?: (value: T) => unknown;
    readonly rejected?: (cause: unknown) => unknown;
  },
): { readonly promise: Promise<R>; readonly certified: boolean } {
  const observer = promise.then;
  const certified = canReplacePhysicalCarrier(promise, observer);
  const handlers = callbacks(certified);
  const fulfilled = ownDescriptor(handlers, "fulfilled");
  const rejected = ownDescriptor(handlers, "rejected");
  const observerArguments =
    rejected !== undefined
      ? [fulfilled?.value, rejected.value]
      : fulfilled !== undefined
        ? [fulfilled.value]
        : [];
  const carrier = nativeApply(observer, promise, observerArguments) as Promise<R>;
  if (certified) retainGenuinePromise(carrier);
  return { promise: carrier, certified };
}

interface CleanupFailure {
  readonly order: number;
  readonly cause: unknown;
  readonly work: PhysicalWork;
}

/** Cleanup detached by one synchronous reset invocation. */
export interface ResetCleanupScope {
  readonly pending: Set<() => void>;
  active: boolean;
}

export function retainResetCleanup(
  scope: ResetCleanupScope | undefined,
  action: () => void,
): () => void {
  if (scope === undefined) return action;
  let pending = true;
  const cleanup = (): void => {
    if (!pending) return;
    pending = false;
    scope.pending.delete(cleanup);
    action();
  };
  scope.pending.add(cleanup);
  return cleanup;
}

/** Native graph/context completion, independent of publication collections. */
export interface RetirementOwner {
  readonly parent?: RetirementOwner;
  readonly work: Set<PhysicalWork>;
  readonly failures: CleanupFailure[];
  nextOrder: number;
  closing: boolean;
  closed: boolean;
  preparedCompletion?: boolean;
  promise?: Promise<void>;
  resolve?: () => void;
  reject?: (cause: unknown) => void;
}

/** One actual native acquisition, original operation or cleanup invocation. */
export interface PhysicalWork {
  readonly owner: RetirementOwner;
  readonly kind: PhysicalWorkKind;
  readonly order: number;
  original?: unknown;
  continuation?: Promise<unknown>;
  active: boolean;
  cleanupFailed: boolean;
}

export function createRetirementOwner(parent?: RetirementOwner): RetirementOwner {
  return {
    parent,
    work: new Set(),
    failures: [],
    nextOrder: 0,
    closing: false,
    closed: false,
  };
}

function rootOwner(owner: RetirementOwner): RetirementOwner {
  return owner.parent === undefined ? owner : rootOwner(owner.parent);
}

function completeRetirement(owner: RetirementOwner): void {
  if (!owner.closed || owner.closing || owner.work.size !== 0 || owner.resolve === undefined)
    return;
  const resolve = owner.resolve;
  const reject = owner.reject!;
  let failures: CleanupFailure[];
  let rejection: unknown;
  try {
    failures = [...owner.failures].sort((left, right) => left.order - right.order);
    if (failures.length === 1) rejection = failures[0]!.cause;
    else if (failures.length > 1)
      rejection = new AggregateError(
        failures.map((failure) => failure.cause),
        "Native retirement cleanup failed",
      );
  } catch (cause) {
    owner.resolve = undefined;
    owner.reject = undefined;
    reject(cause);
    return;
  }
  noteTransferChange(owner);
  owner.resolve = undefined;
  owner.reject = undefined;
  if (failures.length === 0) resolve();
  else reject(rejection);
}

export function beginPhysicalWork(owner: RetirementOwner, kind: PhysicalWorkKind): PhysicalWork {
  const root = rootOwner(owner);
  noteTransferChange(root);
  const order = root.nextOrder++;
  const work: PhysicalWork = {
    owner,
    kind,
    order,
    active: true,
    cleanupFailed: false,
  };
  for (
    let current: RetirementOwner | undefined = owner;
    current !== undefined;
    current = current.parent
  ) {
    noteTransferChange(current);
    current.work.add(work);
  }
  noteTransferChange(work);
  return work;
}

export function finishPhysicalWork(work: PhysicalWork): void {
  if (!work.active) return;
  noteTransferChange(work);
  work.active = false;
  if (!work.cleanupFailed) {
    work.original = undefined;
    work.continuation = undefined;
  }
  for (
    let current: RetirementOwner | undefined = work.owner;
    current !== undefined;
    current = current.parent
  ) {
    noteTransferChange(current);
    current.work.delete(work);
    completeRetirement(current);
  }
  releaseTransferWork(work);
}

export function recordCleanupFailure(work: PhysicalWork, cause: unknown): void {
  if (work.cleanupFailed) return;
  noteTransferChange(work);
  work.cleanupFailed = true;
  const failure = { order: work.order, cause, work };
  for (
    let current: RetirementOwner | undefined = work.owner;
    current !== undefined;
    current = current.parent
  ) {
    noteTransferChange(current);
    current.failures.push(failure);
  }
}

export function observePhysicalWork(
  work: PhysicalWork,
  original: Promise<unknown>,
  cleanup = false,
  source?: unknown,
): void {
  noteTransferChange(work);
  work.original = arguments.length >= 4 ? source : original;
  work.continuation = original;
  void nativeApply(nativeThen, original, [
    () => finishPhysicalWork(work),
    (cause: unknown) => {
      if (cleanup) recordCleanupFailure(work, cause);
      finishPhysicalWork(work);
    },
  ]);
}

export function physicalCleanup(owner: RetirementOwner, action: () => void): void {
  const work = beginPhysicalWork(owner, "cleanup");
  registerTransferWork(owner, work);
  try {
    action();
  } catch (cause) {
    recordCleanupFailure(work, cause);
    throw cause;
  } finally {
    finishPhysicalWork(work);
  }
}

/** @internal Track one positioned reader release without thenable assimilation. */
export function physicalReaderRelease(owner: RetirementOwner, release: () => unknown): void {
  const work = beginPhysicalWork(owner, "cleanup");
  registerTransferWork(owner, work);
  let original: unknown;
  try {
    original = release();
  } catch (cause) {
    recordCleanupFailure(work, cause);
    finishPhysicalWork(work);
    throw cause;
  }
  noteTransferChange(work);
  work.original = original;
  if ((typeof original !== "object" || original === null) && typeof original !== "function") {
    finishPhysicalWork(work);
    return;
  }
  try {
    work.continuation = nativeApply(nativeThen, original, [
      () => finishPhysicalWork(work),
      (cause: unknown) => {
        recordCleanupFailure(work, cause);
        finishPhysicalWork(work);
      },
    ]);
  } catch (cause) {
    recordCleanupFailure(work, cause);
    throw cause;
  }
}

export function retireOwner(
  owner: RetirementOwner,
  dispose: (work: PhysicalWork) => void,
): Promise<void> {
  if (owner.promise !== undefined && owner.preparedCompletion !== true) return owner.promise;
  owner.preparedCompletion = false;
  noteTransferChange(owner);
  owner.promise ??= createPhysicalPromise<void>((resolve, reject) => {
    owner.resolve = resolve;
    owner.reject = reject;
  });
  // Disposal may synchronously reenter retirement before it returns.
  noteTransferChange(owner);
  owner.closing = true;
  const work = beginPhysicalWork(owner, "cleanup");
  try {
    dispose(work);
  } catch (cause) {
    recordCleanupFailure(work, cause);
  } finally {
    finishPhysicalWork(work);
    noteTransferChange(owner);
    owner.closed = true;
    owner.closing = false;
    completeRetirement(owner);
  }
  return owner.promise;
}

/** Track the original iterator return separately from its logical close. */
export function returnPhysicalIterator<T>(
  owner: RetirementOwner,
  iterator: AsyncIterator<T>,
  kind: "stream-return" | "projection-return",
  suppliedClose?: AsyncIterator<T>["return"],
): void {
  const work = beginPhysicalWork(owner, kind);
  let hasOriginal = false;
  try {
    let close: AsyncIterator<T>["return"];
    try {
      close = suppliedClose ?? iterator.return;
    } catch (cause) {
      recordCleanupFailure(work, cause);
      throw cause;
    }
    if (typeof close !== "function") return;
    let original: unknown;
    try {
      original = close.call(iterator);
    } catch (cause) {
      recordCleanupFailure(work, cause);
      return;
    }
    hasOriginal = true;
    work.original = original;
    try {
      observePhysicalWork(work, normalizePhysicalPromise(original), true, original);
    } catch {
      /* The returned original remains unacknowledged. */
    }
  } finally {
    if (!hasOriginal) finishPhysicalWork(work);
  }
}

/** @internal Creates the original completion before a reader session is exposed. */
export function prepareReaderRetirement(
  parent: RetirementOwner,
): RetirementOwner & { readonly promise: Promise<void> } {
  const owner = createRetirementOwner(parent);
  const completion = createPhysicalCompletion<void>();
  owner.promise = completion.promise;
  owner.resolve = completion.resolve;
  owner.reject = completion.reject;
  owner.preparedCompletion = true;
  return owner as RetirementOwner & { readonly promise: Promise<void> };
}

const readerClock = Date.now;
const readerAborted =
  typeof AbortSignal === "undefined"
    ? undefined
    : Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")?.get;
const readerAbortReason =
  typeof AbortSignal === "undefined"
    ? undefined
    : Object.getOwnPropertyDescriptor(AbortSignal.prototype, "reason")?.get;
const readerAddListener =
  typeof EventTarget === "undefined" ? undefined : EventTarget.prototype.addEventListener;
const readerRemoveListener =
  typeof EventTarget === "undefined" ? undefined : EventTarget.prototype.removeEventListener;

/** @internal The platform epoch clock captured before provider callbacks. */
export function readerLifetimeNow(): number {
  return nativeApply(readerClock, undefined, []);
}

export interface PreparedReaderLifetime {
  readonly signal: AbortSignal;
  readonly expiresAt?: number;
  currentAt(now: number): boolean;
  dispose(): void;
}

/** @internal Captures a genuine reservation lifetime without retaining provider getters. */
export function prepareReaderLifetime(
  value: unknown,
  abort: (reason: unknown) => void,
): PreparedReaderLifetime {
  if (
    value === null ||
    typeof value !== "object" ||
    readerAborted === undefined ||
    readerAddListener === undefined ||
    readerRemoveListener === undefined
  )
    throw new TypeError("Required positioned transfer needs a native reservation lifetime");
  const fields = Reflect.ownKeys(value);
  if (fields.some((key) => key !== "signal" && key !== "expiresAt"))
    throw new TypeError("Unknown reader lifetime field");
  const signalField = Object.getOwnPropertyDescriptor(value, "signal");
  const deadlineField = Object.getOwnPropertyDescriptor(value, "expiresAt");
  if (
    signalField === undefined ||
    !("value" in signalField) ||
    (deadlineField !== undefined && !("value" in deadlineField))
  )
    throw new TypeError("Reader lifetime requires own data fields");
  const signal = signalField.value as AbortSignal;
  const expiresAt = deadlineField?.value as number | undefined;
  if (expiresAt !== undefined && (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)))
    throw new TypeError("Reader deadline must be its existing finite epoch deadline");
  if (
    nativeApply(readerAborted, signal, []) !== false ||
    (expiresAt !== undefined && readerLifetimeNow() >= expiresAt)
  )
    throw new TypeError("Reader reservation is already invalid");
  let disposed = false;
  const listener = (): void => {
    if (!disposed)
      abort(
        readerAbortReason === undefined ? undefined : nativeApply(readerAbortReason, signal, []),
      );
  };
  nativeApply(readerAddListener, signal, ["abort", listener, { once: true }]);
  const dispose = (): void => {
    if (disposed) return;
    disposed = true;
    nativeApply(readerRemoveListener, signal, ["abort", listener]);
  };
  if (nativeApply(readerAborted, signal, []) !== false) {
    dispose();
    throw new TypeError("Reader lifetime changed during preparation");
  }
  return Object.freeze({
    signal,
    ...(expiresAt === undefined ? {} : { expiresAt }),
    currentAt: (now: number): boolean =>
      !disposed &&
      nativeApply(readerAborted, signal, readerGetterArguments) === false &&
      (expiresAt === undefined || now < expiresAt),
    dispose,
  });
}

/** @internal Observe a completion whose constructor failed before exposing its resource. */
export function observeUnreturnedReaderRetirement(retired: Promise<void>): void {
  void nativeApply(nativeThen, retired, [undefined, () => undefined]);
}

/** @internal Observe actual reader cleanup without replacing either public completion. */
export function observeReaderCompletion(
  retired: Promise<void>,
  fulfilled: () => void,
  rejected: (cause: unknown) => void,
): void {
  void nativeApply(nativeThen, retired, [fulfilled, rejected]);
}
