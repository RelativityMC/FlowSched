package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;

public class ItemHolder<K, V, Ctx, UserData> {

    public static final IllegalStateException UNLOADED_EXCEPTION = new IllegalStateException("Not loaded");
    private static final CompletableFuture<Void> UNLOADED_FUTURE = CompletableFuture.failedFuture(UNLOADED_EXCEPTION);

    private final K key;
    private final ItemStatus<K, V, Ctx> unloadedStatus;
    private final AtomicReference<V> item = new AtomicReference<>();
    private final AtomicReference<UserData> userData = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> opFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final TicketSet<K, V, Ctx> tickets;
    private final AtomicReference<ItemStatus<K, V, Ctx>> status = new AtomicReference<>();
    private final AtomicReferenceArray<KeyStatusPair<K, V, Ctx>[]> requestedDependencies;
    private final AtomicReferenceArray<CompletableFuture<Void>> futures;

    ItemHolder(ItemStatus<K, V, Ctx> initialStatus, K key) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.status.set(this.unloadedStatus);
        this.key = Objects.requireNonNull(key);
        this.tickets = new TicketSet<>(initialStatus);

        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.futures = new AtomicReferenceArray<>(allStatuses.length);
        this.requestedDependencies = new AtomicReferenceArray<>(allStatuses.length);
        for (int i = 0, allStatusesLength = allStatuses.length; i < allStatusesLength; i++) {
            this.futures.set(i, UNLOADED_FUTURE);
            this.requestedDependencies.set(i, null);
        }
        VarHandle.fullFence();
    }

    private synchronized void createFutures() {
        final ItemStatus<K, V, Ctx> targetStatus = this.getTargetStatus();
        for (int i = this.unloadedStatus.ordinal() + 1; i <= targetStatus.ordinal(); i++) {
            this.futures.getAndUpdate(i, future -> future == UNLOADED_FUTURE ? new CompletableFuture<>() : future);
        }
    }

    /**
     * Get the target status of this item.
     *
     * @return the target status of this item, or null if no ticket is present
     */
    public synchronized ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.tickets.getTargetStatus();
    }

    public synchronized ItemStatus<K, V, Ctx> getStatus() {
        return this.status.get();
    }

    public synchronized boolean isBusy() {
        return !this.opFuture.get().isDone();
    }

    public synchronized void addTicket(ItemTicket<K, V, Ctx> ticket) {
        final boolean add = this.tickets.add(ticket);
        if (!add) {
            throw new IllegalStateException("Ticket already exists");
        }
        createFutures();
        if (ticket.getTargetStatus().ordinal() <= this.getStatus().ordinal()) {
            ticket.consumeCallback();
        }
    }

    public synchronized void removeTicket(ItemTicket<K, V, Ctx> ticket) {
        final boolean remove = this.tickets.remove(ticket);
        if (!remove) {
            throw new IllegalStateException("Ticket does not exist");
        }
        createFutures();
    }

    public void submitOp(CompletionStage<Void> op) {
//        this.opFuture.set(opFuture.get().thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
        this.opFuture.getAndUpdate(future -> future.thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
    }

    public CompletableFuture<Void> getOpFuture() {
        return this.opFuture.get().thenApply(Function.identity());
    }

    public synchronized void setStatus(ItemStatus<K, V, Ctx> status) {
        final ItemStatus<K, V, Ctx> prevStatus = this.getStatus();
        Assertions.assertTrue(status != prevStatus, "duplicate setStatus call");
        this.status.set(status);
        final int compare = Integer.compare(status.ordinal(), prevStatus.ordinal());
        if (compare < 0) { // status downgrade
            Assertions.assertTrue(prevStatus.getPrev() == status, "Invalid status downgrade");

            final ItemStatus<K, V, Ctx> targetStatus = this.getTargetStatus();
            for (int i = prevStatus.ordinal(); i < this.futures.length(); i ++) {
                if (i > targetStatus.ordinal()) {
                    this.futures.getAndSet(i, UNLOADED_FUTURE).completeExceptionally(UNLOADED_EXCEPTION); // unloaded
                } else {
                    this.futures.getAndUpdate(i, future -> future.isDone() ? new CompletableFuture<>() : future);
                }
            }
        } else if (compare > 0) { // status upgrade
            Assertions.assertTrue(prevStatus.getNext() == status, "Invalid status upgrade");

            final CompletableFuture<Void> future = this.futures.get(status.ordinal());

            Assertions.assertTrue(future != UNLOADED_FUTURE);
            Assertions.assertTrue(!future.isDone());
            future.complete(null);
        }
        for (ItemTicket<K, V, Ctx> ticket : this.tickets.getTicketsForStatus(status)) {
            ticket.consumeCallback();
        }
    }

    public synchronized void setDependencies(ItemStatus<K, V, Ctx> status, KeyStatusPair<K, V, Ctx>[] dependencies) {
        if (dependencies != null) {
            final KeyStatusPair<K, V, Ctx>[] result = this.requestedDependencies.compareAndExchange(status.ordinal(), null, dependencies);
            Assertions.assertTrue(result == null, "Duplicate setDependencies call");
        } else {
            final KeyStatusPair<K, V, Ctx>[] result = this.requestedDependencies.getAndSet(status.ordinal(), null);
            Assertions.assertTrue(result != null, "Duplicate setDependencies call");
        }
    }

    public synchronized KeyStatusPair<K, V, Ctx>[] getDependencies(ItemStatus<K, V, Ctx> status) {
        return this.requestedDependencies.get(status.ordinal());
    }

    public synchronized K getKey() {
        return this.key;
    }

    public synchronized CompletableFuture<Void> getFutureForStatus(ItemStatus<K, V, Ctx> status) {
        return this.futures.get(status.ordinal()).thenApply(Function.identity());
    }
    
    public AtomicReference<V> getItem() {
        return this.item;
    }

    public AtomicReference<UserData> getUserData() {
        return this.userData;
    }
}
