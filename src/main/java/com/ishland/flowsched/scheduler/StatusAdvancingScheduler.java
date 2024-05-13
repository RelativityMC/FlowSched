package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scheduler that advances status of items.
 *
 * @param <K> the key type
 * @param <V> the item type
 * @param <Ctx> the context type
 */
public abstract class StatusAdvancingScheduler<K, V, Ctx> {

    private final Object2ReferenceMap<K, ItemHolder<K, V, Ctx>> items = Object2ReferenceMaps.synchronize(new Object2ReferenceOpenHashMap<>());
    private final ObjectArrayFIFOQueue<K> pendingUpdates = new ObjectArrayFIFOQueue<>();

    protected abstract Executor getExecutor();

    protected abstract ItemStatus<Ctx> getUnloadedStatus();

    /**
     * Get the dependencies of the given item at the given status.
     * <p>
     * The returned collection must not contain the given item itself.
     *
     * @param holder the item holder
     * @param status the status
     * @return the dependencies
     */
    protected abstract Collection<KeyStatusPair<K, Ctx>> getDependencies(ItemHolder<K, V, Ctx> holder, ItemStatus<Ctx> status);

    protected abstract Ctx makeContext(ItemHolder<K, V, Ctx> holder, ItemStatus<Ctx> nextStatus);

    public boolean tick() {
        boolean hasWork = false;
        while (!this.pendingUpdates.isEmpty()) {
            hasWork = true;
            K key = this.pendingUpdates.dequeue();
            ItemHolder<K, V, Ctx> holder = this.items.get(key);
            if (holder == null) {
                continue;
            }
            if (holder.isBusy()) {
                continue;
            }
            final ItemStatus<Ctx> current = holder.getStatus();
            ItemStatus<Ctx> nextStatus = getNextStatus(current, holder.getTargetStatus());
            if (nextStatus == current) {
                if (current.equals(getUnloadedStatus())) {
//                    System.out.println("Unloaded: " + key);
                    this.items.remove(key);
                }
                continue; // No need to update
            }
            if (((Comparable<ItemStatus<Ctx>>) current).compareTo(nextStatus) < 0) {
                // Advance
                final Collection<KeyStatusPair<K, Ctx>> dependencies = getDependencies(holder, nextStatus);
                holder.setDependencies(nextStatus, dependencies);
                final CompletableFuture<Void> dependencyFuture = getDependencyFuture0(dependencies, key);
                holder.submitOp(dependencyFuture.thenCompose(unused -> {
                    final Ctx ctx = makeContext(holder, nextStatus);
                    return nextStatus.upgradeToThis(ctx);
                }).whenCompleteAsync((unused, throwable) -> {
                    // TODO exception handling
                    holder.setStatus(nextStatus);
                    markDirty(key);
                }, getExecutor()).whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                    }
                }));
            } else {
                // Downgrade
                final Collection<KeyStatusPair<K, Ctx>> dependencies = holder.getDependencies(current);
                Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");
                holder.setDependencies(current, null);
                final Ctx ctx = makeContext(holder, current);
                holder.submitOp(current.downgradeFromThis(ctx).whenCompleteAsync((unused, throwable) -> {
                    // TODO exception handling
                    holder.setStatus(nextStatus);
                    for (KeyStatusPair<K, Ctx> dependency : dependencies) {
                        this.removeTicketWithSource(dependency.key(), key, dependency.status());
                    }
                    markDirty(key);
                }, getExecutor()));
            }
        }
        return hasWork;
    }

    public ItemHolder<K, V, Ctx> getHolder(K key) {
        return this.items.get(key);
    }

    public int itemCount() {
        return this.items.size();
    }

    protected void markDirty(K key) {
        this.pendingUpdates.enqueue(key);
    }

    private CompletableFuture<Void> getDependencyFuture0(Collection<KeyStatusPair<K, Ctx>> dependencies, K key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicInteger satisfied = new AtomicInteger(0);
        final int size = dependencies.size();
        if (size == 0) {
            return CompletableFuture.completedFuture(null);
        }
        for (KeyStatusPair<K, Ctx> dependency : dependencies) {
            Assertions.assertTrue(!dependency.key().equals(key));
            this.addTicketWithSource(dependency.key(), key, dependency.status(), () -> {
                final int incrementAndGet = satisfied.incrementAndGet();
                Assertions.assertTrue(incrementAndGet <= size, "Satisfied more than expected");
                if (incrementAndGet == size) {
                    future.complete(null);
                }
            });
        }
        return future;
    }

    public void addTicket(K pos, ItemStatus<Ctx> targetStatus, Runnable callback) {
        this.getExecutor().execute(() -> this.addTicketWithSource(pos, pos, targetStatus, callback));
    }

    private void addTicketWithSource(K pos, K source, ItemStatus<Ctx> targetStatus, Runnable callback) {
        ItemHolder<K, V, Ctx> holder = this.items.computeIfAbsent(pos, (K k) -> new ItemHolder<>(this.getUnloadedStatus(), k));
        if (this.getUnloadedStatus().equals(targetStatus)) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        holder.addTicket(new ItemTicket<>(source, targetStatus, callback));
        markDirty(pos);
    }

    public void removeTicket(K pos, ItemStatus<Ctx> targetStatus) {
        this.getExecutor().execute(() -> this.removeTicketWithSource(pos, pos, targetStatus));
    }

    private void removeTicketWithSource(K pos, K source, ItemStatus<Ctx> targetStatus) {
        ItemHolder<K, V, Ctx> holder = this.items.get(pos);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.removeTicket(new ItemTicket<>(source, targetStatus, null));
        markDirty(pos);
    }

    private ItemStatus<Ctx> getNextStatus(ItemStatus<Ctx> current, ItemStatus<Ctx> target) {
        Assertions.assertTrue(target != null);
        final int compare = ((Comparable<ItemStatus<Ctx>>) current).compareTo(target);
        if (compare < 0) {
            return current.getNext();
        } else if (compare == 0) {
            return current;
        } else {
            return current.getPrev();
        }
    }

    protected boolean hasPendingUpdates() {
        return !this.pendingUpdates.isEmpty();
    }

}
