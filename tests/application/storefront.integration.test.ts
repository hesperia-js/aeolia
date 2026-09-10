import { expect, test } from "bun:test";
import {
  affects,
  createGraph,
  createMutation,
  createQuery,
  createStream,
  defineContract,
  project,
  subscribe,
  type Readable,
} from "../../src/index.ts";
import { testBackend, type CallRecord, type TestBackend } from "../../src/testing.ts";

type Product = Readonly<{
  id: string;
  name: string;
  price: number;
}>;

type Inventory = Readonly<{
  productId: string;
  available: number;
}>;

type CartLine = Readonly<{
  productId: string;
  quantity: number;
}>;

type Cart = readonly CartLine[];

type Order = Readonly<{
  id: string;
  userId: string;
  productId: string;
  quantity: number;
}>;

type AddToCartInput = Readonly<{
  userId: string;
  line: CartLine;
}>;

type PurchaseInput = Readonly<{
  userId: string;
  productId: string;
  quantity: number;
}>;

type InventoryEvent = Readonly<{
  productId: string;
  available: number;
}>;

type OrderEvent = Readonly<{
  orderId: string;
  userId: string;
  kind: "paid";
}>;

type OrderSummary = Readonly<{
  seen: number;
  paid: number;
}>;

function pendingCall(backend: TestBackend, name: string, key?: string): CallRecord {
  const call = backend
    .pending(name)
    .find((candidate) => key === undefined || String(candidate.key) === key);
  if (call === undefined) throw new Error(`Expected pending ${name} call`);
  return call;
}

function waitFor<T>(readable: Readable<T>, predicate: (value: T) => boolean): Promise<void> {
  return new Promise((resolve) => {
    let done = false;
    let stop: (() => void) | undefined;
    const finish = (): void => {
      if (done) return;
      done = true;
      stop?.();
      resolve();
    };
    stop = subscribe(readable, (value) => {
      if (predicate(value)) finish();
    });
    if (done) stop();
  });
}

function sameCart(left: Cart, right: Cart): boolean {
  return (
    left.length === right.length &&
    left.every(
      (line, index) =>
        line.productId === right[index]?.productId && line.quantity === right[index]?.quantity,
    )
  );
}

test("runs a storefront flow through joined queries, effects, streams, and disposal", async () => {
  const backend = testBackend();
  const productDefinition = createQuery({
    name: "product.get",
    key: (productId: string) => `product/${productId}`,
    fetch: backend.respond<string, Product>("product.get"),
  });
  const inventoryDefinition = createQuery({
    name: "inventory.get",
    key: (productId: string) => `inventory/${productId}`,
    fetch: backend.respond<string, Inventory>("inventory.get"),
  });
  const cartDefinition = createQuery({
    name: "cart.get",
    key: (userId: string) => `cart/${userId}`,
    equals: sameCart,
    fetch: backend.respond<string, Cart>("cart.get"),
  });
  const historyDefinition = createQuery({
    name: "orders.history",
    key: (userId: string) => `orders/${userId}`,
    fetch: backend.respond<string, readonly Order[]>("orders.history"),
  });
  const inventoryEventsDefinition = createStream({
    name: "inventory.events",
    key: (productId: string) => `inventory-events/${productId}`,
    open: backend.stream<string, InventoryEvent>("inventory.events"),
  });
  const orderEventsDefinition = createStream({
    name: "order.events",
    key: (userId: string) => `order-events/${userId}`,
    open: backend.stream<string, OrderEvent>("order.events"),
  });
  const addToCartDefinition = createMutation<AddToCartInput, void>({
    name: "cart.add",
    run: backend.perform<AddToCartInput, void>("cart.add"),
    affects: [
      affects(cartDefinition, {
        select: (input) => input.userId,
        on: "revalidate",
        optimistic: (current, input) => [...(current ?? []), input.line],
      }),
    ],
  });
  const purchaseDefinition = createMutation<PurchaseInput, Order>({
    name: "order.purchase",
    run: backend.perform<PurchaseInput, Order>("order.purchase"),
    affects: [
      affects(inventoryDefinition, {
        select: (input) => input.productId,
        on: "revalidate",
      }),
      affects(cartDefinition, {
        select: (input) => input.userId,
        on: "revalidate",
      }),
      affects(historyDefinition, {
        select: (input) => input.userId,
        on: "revalidate",
      }),
    ],
  });
  const contract = defineContract({
    namespace: "storefront-flow",
    operations: {
      product: productDefinition,
      inventory: inventoryDefinition,
      cart: cartDefinition,
      history: historyDefinition,
      inventoryEvents: inventoryEventsDefinition,
      orderEvents: orderEventsDefinition,
      addToCart: addToCartDefinition,
      purchase: purchaseDefinition,
    },
  });
  const graph = createGraph({ contract });

  const product = graph.api.product("sku-1");
  const joinedProduct = graph.api.product("sku-1");
  const inventory = graph.api.inventory("sku-1");
  const cart = graph.api.cart("user-1");
  const history = graph.api.history("user-1");
  const productValues: Product[] = [];
  const cartValues: Cart[] = [];
  const stopProduct = subscribe(product, (value) => productValues.push(value));
  const stopCart = subscribe(cart, (value) => cartValues.push(value));

  try {
    expect(joinedProduct.value).toBe(product.value);
    expect(joinedProduct.key).toBe(product.key);
    expect(backend.pending("product.get")).toHaveLength(1);
    expect(backend.pending()).toHaveLength(4);
    expect(productValues).toStrictEqual([]);
    expect(cartValues).toStrictEqual([]);

    const initialProduct: Product = { id: "sku-1", name: "Copper kettle", price: 48 };
    const initialInventory: Inventory = { productId: "sku-1", available: 8 };
    const initialCart: Cart = [];
    const initialHistory: readonly Order[] = [];
    backend.resolve(pendingCall(backend, "product.get", "product/sku-1"), initialProduct);
    backend.resolve(pendingCall(backend, "inventory.get", "inventory/sku-1"), initialInventory);
    backend.resolve(pendingCall(backend, "cart.get", "cart/user-1"), initialCart);
    backend.resolve(pendingCall(backend, "orders.history", "orders/user-1"), initialHistory);
    await Promise.all([product.ready, inventory.ready, cart.ready, history.ready]);
    expect(product.value.get()).toEqual(initialProduct);
    expect(joinedProduct.value.get()).toEqual(initialProduct);
    expect(inventory.value.get()).toEqual(initialInventory);
    expect(cart.value.get()).toEqual(initialCart);
    expect(history.value.get()).toEqual(initialHistory);
    expect(productValues).toStrictEqual([initialProduct]);
    expect(cartValues).toStrictEqual([initialCart]);

    const line: CartLine = { productId: "sku-1", quantity: 1 };
    const add = graph.api.addToCart({ userId: "user-1", line });
    expect(cart.value.get()).toEqual([line]);
    expect(cartValues).toStrictEqual([initialCart, [line]]);
    backend.resolve(pendingCall(backend, "cart.add"), undefined);
    await add;
    expect(cart.status.get()).toBe("revalidating");
    const cartAfterAddReady = cart.ready;
    const equalAuthoritativeCart: Cart = [{ ...line }];
    const cartNotificationsAfterPrediction = cartValues.length;
    backend.resolve(pendingCall(backend, "cart.get", "cart/user-1"), equalAuthoritativeCart);
    await expect(cartAfterAddReady).resolves.toEqual(equalAuthoritativeCart);
    expect(cartValues).toHaveLength(cartNotificationsAfterPrediction);
    expect(cart.value.get()).toEqual(equalAuthoritativeCart);

    const purchaseInput: PurchaseInput = { userId: "user-1", productId: "sku-1", quantity: 1 };
    const purchase = graph.api.purchase(purchaseInput);
    const receipt: Order = {
      id: "order-1",
      userId: "user-1",
      productId: "sku-1",
      quantity: 1,
    };
    backend.resolve(pendingCall(backend, "order.purchase"), receipt);
    await expect(purchase).resolves.toEqual(receipt);
    expect(product.value.get()).toEqual(initialProduct);
    expect(product.status.get()).toBe("ready");
    expect(backend.pending("product.get")).toHaveLength(0);
    expect(inventory.status.get()).toBe("revalidating");
    expect(cart.status.get()).toBe("revalidating");
    expect(history.status.get()).toBe("revalidating");

    const failedInventoryRefresh = new Error("inventory service unavailable");
    const inventoryReadyAfterFailure = inventory.ready;
    const refreshedInventory: Inventory = { productId: "sku-1", available: 7 };
    const refreshedHistory: readonly Order[] = [receipt];
    backend.resolve(pendingCall(backend, "cart.get", "cart/user-1"), []);
    backend.resolve(pendingCall(backend, "orders.history", "orders/user-1"), refreshedHistory);
    backend.reject(
      pendingCall(backend, "inventory.get", "inventory/sku-1"),
      failedInventoryRefresh,
    );
    await expect(inventoryReadyAfterFailure).rejects.toBe(failedInventoryRefresh);
    await expect(cart.ready).resolves.toEqual([]);
    await expect(history.ready).resolves.toEqual(refreshedHistory);
    expect(inventory.value.get()).toEqual(initialInventory);
    expect(inventory.error.get()).toBe(failedInventoryRefresh);
    expect(inventory.status.get()).toBe("failed");
    expect(cart.value.get()).toEqual([]);
    expect(history.value.get()).toEqual(refreshedHistory);
    expect(productValues).toStrictEqual([initialProduct]);

    const inventoryRetry = inventory.revalidate();
    const inventoryRetryReady = inventory.ready;
    backend.resolve(pendingCall(backend, "inventory.get", "inventory/sku-1"), refreshedInventory);
    await inventoryRetry;
    await expect(inventoryRetryReady).resolves.toEqual(refreshedInventory);
    expect(inventory.value.get()).toEqual(refreshedInventory);
    expect(inventory.error.get()).toBeUndefined();

    const inventoryStream = graph.api.inventoryEvents("sku-1");
    const orderStream = graph.api.orderEvents("user-1");
    const inventoryLog = project(graph, inventoryEventsDefinition, "sku-1", {
      kind: "accumulate",
      max: 2,
      onOverflow: "drop-oldest",
    });
    const orderSummary = project(graph, orderEventsDefinition, "user-1", {
      kind: "reduce",
      initial: { seen: 0, paid: 0 } satisfies OrderSummary,
      step: (summary, event): OrderSummary => ({
        seen: summary.seen + 1,
        paid: summary.paid + (event.kind === "paid" ? 1 : 0),
      }),
    });
    const inventoryLogs: Array<readonly InventoryEvent[]> = [];
    const summaries: OrderSummary[] = [];
    const inventoryValues: InventoryEvent[] = [];
    const orderValues: OrderEvent[] = [];
    const stopInventory = subscribe(inventoryStream, (value) => inventoryValues.push(value));
    const stopOrder = subscribe(orderStream, (value) => orderValues.push(value));
    const stopInventoryLog = subscribe(inventoryLog, (value) => inventoryLogs.push(value));
    const stopSummary = subscribe(orderSummary, (value) => summaries.push(value));
    expect(inventoryStream.status.get()).toBe("opening");
    expect(orderStream.status.get()).toBe("opening");
    expect(inventoryValues).toStrictEqual([]);
    expect(orderValues).toStrictEqual([]);
    expect(inventoryLogs).toStrictEqual([[]]);
    expect(summaries).toStrictEqual([{ seen: 0, paid: 0 }]);
    expect(backend.pending("inventory.events")).toHaveLength(2);
    expect(backend.pending("order.events")).toHaveLength(2);

    const inventoryEvent1: InventoryEvent = { productId: "sku-1", available: 7 };
    const inventoryEvent2: InventoryEvent = { productId: "sku-1", available: 6 };
    const orderEvent: OrderEvent = { orderId: "order-1", userId: "user-1", kind: "paid" };
    const firstInventory = waitFor(inventoryLog.value, (value) => value.length === 1);
    backend.emit("inventory.events", inventoryEvent1);
    await firstInventory;
    const secondInventory = waitFor(inventoryLog.value, (value) => value.length === 2);
    backend.emit("inventory.events", inventoryEvent1);
    await secondInventory;
    const thirdInventory = waitFor(
      inventoryLog.value,
      (value) => value.length === 2 && value[0] === inventoryEvent1 && value[1] === inventoryEvent2,
    );
    backend.emit("inventory.events", inventoryEvent2);
    await thirdInventory;
    const firstOrder = waitFor(orderSummary.value, (value) => value.seen === 1);
    backend.emit("order.events", orderEvent);
    await firstOrder;
    const secondOrder = waitFor(orderSummary.value, (value) => value.seen === 2);
    backend.emit("order.events", orderEvent);
    await secondOrder;
    expect(inventoryStream.value.get()).toEqual(inventoryEvent2);
    expect(inventoryStream.status.get()).toBe("live");
    expect(inventoryValues).toStrictEqual([inventoryEvent1, inventoryEvent2]);
    expect(orderValues).toStrictEqual([orderEvent]);
    expect(inventoryLogs).toStrictEqual([
      [],
      [inventoryEvent1],
      [inventoryEvent1, inventoryEvent1],
      [inventoryEvent1, inventoryEvent2],
    ]);
    expect(orderStream.value.get()).toEqual(orderEvent);
    expect(orderStream.status.get()).toBe("live");
    expect(summaries).toStrictEqual([
      { seen: 0, paid: 0 },
      { seen: 1, paid: 1 },
      { seen: 2, paid: 2 },
    ]);
    stopInventoryLog();
    stopSummary();
    stopInventory();
    stopOrder();

    const pendingInventory = graph.api.inventory("sku-2");
    const pendingReady = pendingInventory.ready;
    const pendingValues: Array<Inventory | undefined> = [];
    const stopPending = subscribe(pendingInventory, (value) => pendingValues.push(value));
    const pendingInventoryCall = pendingCall(backend, "inventory.get", "inventory/sku-2");
    const streamCalls = [
      ...backend.pending("inventory.events"),
      ...backend.pending("order.events"),
    ];
    const pendingRejection = pendingReady.then(
      () => {
        throw new Error("Disposed readiness unexpectedly resolved");
      },
      (error: unknown) => {
        expect(error).toMatchObject({ kind: "disposed" });
      },
    );
    graph.dispose();
    expect(pendingInventoryCall.aborted).toBe(true);
    expect(streamCalls.every((call) => call.aborted)).toBe(true);
    backend.resolve(pendingInventoryCall, { productId: "sku-2", available: 99 });
    backend.emit("inventory.events", { productId: "sku-1", available: 5 });
    await pendingRejection;
    await Promise.resolve();
    expect(pendingValues).toStrictEqual([]);
    expect(inventoryLog.status.get()).toBe("closed");
    expect(orderSummary.status.get()).toBe("closed");
    stopPending();
  } finally {
    stopProduct();
    stopCart();
    graph.dispose();
  }
});
