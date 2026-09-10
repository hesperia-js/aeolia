/**
 * Runtime brand shared by every readable created by Aeolia.
 *
 * Keep this symbol in its own module. All public entry points, including the
 * compatibility facade and direct subsystem imports, must use this identity.
 */
export const readableBrand: unique symbol = Symbol("aeolia.readable");

/** Option key for a callback invoked when a readable gains its first live descendant. */
export const watched: unique symbol = Symbol("Signal.subtle.watched");

/** Option key for a callback invoked after a readable loses its last live descendant. */
export const unwatched: unique symbol = Symbol("Signal.subtle.unwatched");
