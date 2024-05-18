package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ItemHolder<K, V, Ctx> {

    private final CompletableFuture<Void> UNLOADED_FUTURE = CompletableFuture.failedFuture(new IllegalStateException("Not loaded"));

    private final K key;
    private final ItemStatus<K, V, Ctx> unloadedStatus;
    private final AtomicReference<V> item = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> opFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final TicketSet<K, V, Ctx> tickets;
    private final AtomicReference<ItemStatus<K, V, Ctx>> status = new AtomicReference<>();
    private final AtomicReference<Collection<KeyStatusPair<K, V, Ctx>>>[] requestedDependencies;
    private final AtomicReference<CompletableFuture<Void>>[] futures;

    ItemHolder(ItemStatus<K, V, Ctx> initialStatus, K key) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.status.set(this.unloadedStatus);
        this.key = Objects.requireNonNull(key);
        this.tickets = new TicketSet<>(initialStatus);

        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.futures = new AtomicReference[allStatuses.length];
        this.requestedDependencies = new AtomicReference[allStatuses.length];
        for (int i = 0, allStatusesLength = allStatuses.length; i < allStatusesLength; i++) {
            futures[i] = new AtomicReference<>(UNLOADED_FUTURE);
            requestedDependencies[i] = new AtomicReference<>(null);
        }
        VarHandle.fullFence();
    }

    private void updateFutures() {
        final ItemStatus<K, V, Ctx> targetStatus = this.getTargetStatus();
        AtomicReference<CompletableFuture<Void>>[] atomicReferences = this.futures;
        for (int i = 0, atomicReferencesLength = atomicReferences.length; i < atomicReferencesLength; i++) {
            AtomicReference<CompletableFuture<Void>> ref = atomicReferences[i];
            if (i <= targetStatus.ordinal()) {
                ref.getAndUpdate(future -> future == UNLOADED_FUTURE ? new CompletableFuture<>() : future);
            }
        }
    }

    /**
     * Get the target status of this item.
     *
     * @return the target status of this item, or null if no ticket is present
     */
    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.tickets.getTargetStatus();
    }

    public ItemStatus<K, V, Ctx> getStatus() {
        return this.status.get();
    }

    public boolean isBusy() {
        return !this.opFuture.get().isDone();
    }

    public void addTicket(ItemTicket<K, V, Ctx> ticket) {
        final boolean add = this.tickets.add(ticket);
        if (!add) {
            throw new IllegalStateException("Ticket already exists");
        }
        updateFutures();
        if (ticket.getTargetStatus().ordinal() <= this.getStatus().ordinal()) {
            ticket.consumeCallback();
        }
    }

    public void removeTicket(ItemTicket<K, V, Ctx> ticket) {
        final boolean remove = this.tickets.remove(ticket);
        if (!remove) {
            throw new IllegalStateException("Ticket does not exist");
        }
        updateFutures();
    }

    public void submitOp(CompletionStage<Void> op) {
//        this.opFuture.set(opFuture.get().thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
        this.opFuture.getAndUpdate(future -> future.thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
    }

    public void setStatus(ItemStatus<K, V, Ctx> status) {
        final ItemStatus<K, V, Ctx> prevStatus = this.getStatus();
        Assertions.assertTrue(status != prevStatus, "duplicate setStatus call");
        this.status.set(status);
        final int compare = Integer.compare(status.ordinal(), prevStatus.ordinal());
        if (compare < 0) { // status downgrade
            Assertions.assertTrue(prevStatus.getPrev() == status, "Invalid status downgrade");
            this.futures[prevStatus.ordinal()].set(UNLOADED_FUTURE);
        } else if (compare > 0) { // status upgrade
            Assertions.assertTrue(prevStatus.getNext() == status, "Invalid status upgrade");

            final CompletableFuture<Void> future = this.futures[status.ordinal()].get();

            // If the future is set to UNLOADED_FUTURE, the upgrade is cancelled but finished anyway
            // If it isn't, fire the future

            if (future != UNLOADED_FUTURE) {
                Assertions.assertTrue(!future.isDone());
                future.complete(null);
            }
        }
        for (ItemTicket<K, V, Ctx> ticket : this.tickets.getTicketsForStatus(status)) {
            ticket.consumeCallback();
        }
    }

    public void setDependencies(ItemStatus<K, V, Ctx> status, Collection<KeyStatusPair<K, V, Ctx>> dependencies) {
        final AtomicReference<Collection<KeyStatusPair<K, V, Ctx>>> reference = this.requestedDependencies[status.ordinal()];
        if (dependencies != null) {
            final Collection<KeyStatusPair<K, V, Ctx>> result = reference.compareAndExchange(null, dependencies);
            Assertions.assertTrue(result == null, "Duplicate setDependencies call");
        } else {
            final Collection<KeyStatusPair<K, V, Ctx>> result = reference.getAndSet(null);
            Assertions.assertTrue(result != null, "Duplicate setDependencies call");
        }
    }

    public Collection<KeyStatusPair<K, V, Ctx>> getDependencies(ItemStatus<K, V, Ctx> status) {
        return this.requestedDependencies[status.ordinal()].get();
    }

    public K getKey() {
        return this.key;
    }

    public CompletableFuture<Void> getFutureForStatus(ItemStatus<K, V, Ctx> status) {
        return this.futures[status.ordinal()].get().thenApply(Function.identity());
    }
    
    public AtomicReference<V> getItem() {
        return this.item;
    }
}
