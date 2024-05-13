package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.Object2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ItemHolder<K, V, Ctx> {

    private final CompletableFuture<Void> UNLOADED_FUTURE = CompletableFuture.failedFuture(new IllegalStateException("Not loaded"));

    private final K key;
    private final ItemStatus<Ctx> unloadedStatus;
    private final AtomicReference<V> item = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> opFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final ObjectSortedSet<ItemTicket<K, Ctx>> tickets = new ObjectAVLTreeSet<>();
    private final AtomicReference<ItemStatus<Ctx>> status = new AtomicReference<>();
    private final Object2ReferenceMap<ItemStatus<Ctx>, AtomicReference<Collection<KeyStatusPair<K, Ctx>>>> requestedDependencies = new Object2ReferenceOpenHashMap<>();
    private final Object2ReferenceMap<ItemStatus<Ctx>, AtomicReference<CompletableFuture<Void>>> futures = new Object2ReferenceLinkedOpenHashMap<>();

    ItemHolder(ItemStatus<Ctx> initialStatus, K key) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.status.set(this.unloadedStatus);
        this.key = Objects.requireNonNull(key);
        initFutures(initialStatus);
    }

    private void initFutures(ItemStatus<Ctx> initialStatus) {
        for (ItemStatus<Ctx> status : initialStatus.getAllStatuses()) {
            this.futures.put(status, new AtomicReference<>(UNLOADED_FUTURE));
            this.requestedDependencies.put(status, new AtomicReference<>(null));
        }
    }

    private void updateFutures() {
        final ItemStatus<Ctx> targetStatus = this.getTargetStatus();
        for (Map.Entry<ItemStatus<Ctx>, AtomicReference<CompletableFuture<Void>>> entry : this.futures.entrySet()) {
            if (((Comparable<ItemStatus<Ctx>>) entry.getKey()).compareTo(targetStatus) <= 0) {
                entry.getValue().getAndUpdate(future -> future == UNLOADED_FUTURE ? new CompletableFuture<>() : future);
            }
        }
    }

    /**
     * Get the target status of this item.
     *
     * @return the target status of this item, or null if no ticket is present
     */
    public ItemStatus<Ctx> getTargetStatus() {
        return !this.tickets.isEmpty() ? this.tickets.last().getTargetStatus() : unloadedStatus;
    }

    public ItemStatus<Ctx> getStatus() {
        return this.status.get();
    }

    public boolean isBusy() {
        return !this.opFuture.get().isDone();
    }

    public void addTicket(ItemTicket<K, Ctx> ticket) {
        final boolean add = this.tickets.add(ticket);
        if (!add) {
            throw new IllegalStateException("Ticket already exists");
        }
        updateFutures();
        if (((Comparable<ItemStatus<Ctx>>) ticket.getTargetStatus()).compareTo(this.getStatus()) <= 0) {
            ticket.consumeCallback();
        }
    }

    public void removeTicket(ItemTicket<K, Ctx> ticket) {
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

    public void setStatus(ItemStatus<Ctx> status) {
        final ItemStatus<Ctx> prevStatus = this.getStatus();
        Assertions.assertTrue(status != prevStatus, "duplicate setStatus call");
        this.status.set(status);
        final int compare = ((Comparable<ItemStatus<Ctx>>) status).compareTo(prevStatus);
        if (compare < 0) { // status downgrade
            Assertions.assertTrue(prevStatus.getPrev() == status, "Invalid status downgrade");
            this.futures.get(prevStatus).set(UNLOADED_FUTURE);
        } else if (compare > 0) { // status upgrade
            Assertions.assertTrue(prevStatus.getNext() == status, "Invalid status upgrade");

            final CompletableFuture<Void> future = this.futures.get(status).get();

            // If the future is set to UNLOADED_FUTURE, the upgrade is cancelled but finished anyway
            // If it isn't, fire the future

            if (future != UNLOADED_FUTURE) {
                Assertions.assertTrue(!future.isDone());
                future.complete(null);
            }
        }
        for (ItemTicket<K, Ctx> ticket : this.tickets) {
            if (((Comparable<ItemStatus<Ctx>>) ticket.getTargetStatus()).compareTo(status) <= 0) {
                ticket.consumeCallback();
            } else {
                break;
            }
        }
    }

    public void setDependencies(ItemStatus<Ctx> status, Collection<KeyStatusPair<K, Ctx>> dependencies) {
        final AtomicReference<Collection<KeyStatusPair<K, Ctx>>> reference = this.requestedDependencies.get(status);
        if (dependencies != null) {
            final Collection<KeyStatusPair<K, Ctx>> result = reference.compareAndExchange(null, dependencies);
            Assertions.assertTrue(result == null, "Duplicate setDependencies call");
        } else {
            final Collection<KeyStatusPair<K, Ctx>> result = reference.getAndSet(null);
            Assertions.assertTrue(result != null, "Duplicate setDependencies call");
        }
    }

    public Collection<KeyStatusPair<K, Ctx>> getDependencies(ItemStatus<Ctx> status) {
        return this.requestedDependencies.get(status).get();
    }

    public K getKey() {
        return this.key;
    }

    public CompletableFuture<Void> getFutureForStatus(ItemStatus<Ctx> status) {
        return this.futures.get(status).get().thenApply(Function.identity());
    }
}
