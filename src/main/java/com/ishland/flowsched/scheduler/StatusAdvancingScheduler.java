package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
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
 */
public abstract class StatusAdvancingScheduler<K, V, Ctx> {

    private final Object2ReferenceOpenHashMap<K, ItemHolder<K, V, Ctx>> items = new Object2ReferenceOpenHashMap<>();
    private final ObjectArrayFIFOQueue<K> pendingUpdates = new ObjectArrayFIFOQueue<>();

    protected abstract Executor getExecutor();

    protected abstract ItemStatus<Ctx> getInitialStatus();

    /**
     * Get the dependencies of the given item at the given status.
     * <p>
     * The returned collection is reproducible, i.e. the same collection is returned for the same item and status.
     * The returned collection must not contain the given item itself.
     *
     * @param holder the item holder
     * @param status the status
     * @return the dependencies
     */
    protected abstract Collection<KeyStatusPair<K, Ctx>> getDependencies(ItemHolder<K, V, Ctx> holder, ItemStatus<Ctx> status);

    protected abstract Ctx makeContext(ItemHolder<K, V, Ctx> holder, ItemStatus<Ctx> nextStatus);

    public void tick() {
        while (!this.pendingUpdates.isEmpty()) {
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
                continue; // No need to update
            }
            final Collection<KeyStatusPair<K, Ctx>> dependencies = getDependencies(holder, nextStatus);
            if (current.compareTo(nextStatus) < 0) {
                // Advance
                final CompletableFuture<Void> dependencyFuture = getDependencyFuture0(dependencies, key);
                holder.submitOp(dependencyFuture.thenCompose(unused -> {
                    final Ctx ctx = makeContext(holder, nextStatus);
                    return nextStatus.upgradeToThis(ctx);
                }).whenCompleteAsync((unused, throwable) -> {
                    // TODO exception handling
                    holder.setStatus(nextStatus);
                }, getExecutor()));
            } else {
                // Downgrade
                final Ctx ctx = makeContext(holder, nextStatus);
                holder.submitOp(nextStatus.downgradeFromThis(ctx).whenCompleteAsync((unused, throwable) -> {
                    // TODO exception handling
                    holder.setStatus(nextStatus);
                    for (KeyStatusPair<K, Ctx> dependency : dependencies) {
                        this.removeTicketWithSource(dependency.key(), key, dependency.status());
                    }
                }, getExecutor()));
            }
        }
    }

    private CompletableFuture<Void> getDependencyFuture0(Collection<KeyStatusPair<K, Ctx>> dependencies, K key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicInteger satisfied = new AtomicInteger(0);
        final int size = dependencies.size();
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
        this.addTicketWithSource(pos, pos, targetStatus, callback);
    }

    private void addTicketWithSource(K pos, K source, ItemStatus<Ctx> targetStatus, Runnable callback) {
        ItemHolder<K, V, Ctx> holder = this.items.computeIfAbsent(pos, (K k) -> new ItemHolder<>(this.getInitialStatus(), k));
        holder.addTicket(new ItemTicket<>(source, targetStatus, callback));
        this.pendingUpdates.enqueue(pos);
    }

    public void removeTicket(K pos, ItemStatus<Ctx> targetStatus) {
        this.removeTicketWithSource(pos, pos, targetStatus);
    }

    private void removeTicketWithSource(K pos, K source, ItemStatus<Ctx> targetStatus) {
        ItemHolder<K, V, Ctx> holder = this.items.get(pos);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.removeTicket(new ItemTicket<>(source, targetStatus, null));
        this.pendingUpdates.enqueue(pos);
    }

    private ItemStatus<Ctx> getNextStatus(ItemStatus<Ctx> current, ItemStatus<Ctx> target) {
        final int compare = current.compareTo(target);
        if (compare < 0) {
            return current.getNext();
        } else if (compare == 0) {
            return current;
        } else {
            return current.getPrev();
        }
    }

}
