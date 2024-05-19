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

    protected abstract ItemStatus<K, V, Ctx> getUnloadedStatus();

    protected abstract Ctx makeContext(ItemHolder<K, V, Ctx> holder, ItemStatus<K, V, Ctx> nextStatus, boolean isUpgrade);

    protected void onItemCreation(ItemHolder<K, V, Ctx> holder) {
    }

    protected void onItemRemoval(ItemHolder<K, V, Ctx> holder) {
    }

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
            final ItemStatus<K, V, Ctx> current = holder.getStatus();
            ItemStatus<K, V, Ctx> nextStatus = getNextStatus(current, holder.getTargetStatus());
            if (nextStatus == current) {
                if (current.equals(getUnloadedStatus())) {
//                    System.out.println("Unloaded: " + key);
                    this.onItemRemoval(holder);
                    this.items.remove(key);
                }
                continue; // No need to update
            }
            if (current.ordinal() < nextStatus.ordinal()) {
                advanceStatus0(holder, nextStatus, key);
            } else {
                downgradeStatus0(holder, current, nextStatus, key);
            }
        }
        return hasWork;
    }

    private void downgradeStatus0(ItemHolder<K, V, Ctx> holder, ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Downgrade
        final Collection<KeyStatusPair<K, V, Ctx>> dependencies = holder.getDependencies(current);
        Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");
        holder.setDependencies(current, null);
        final Ctx ctx = makeContext(holder, current, false);
        holder.submitOp(current.downgradeFromThis(ctx).whenCompleteAsync((unused, throwable) -> {
            // TODO exception handling
            holder.setStatus(nextStatus);
            for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
                this.removeTicketWithSource(dependency.key(), key, dependency.status());
            }
            markDirty(key);
        }, getExecutor()));
    }

    private void advanceStatus0(ItemHolder<K, V, Ctx> holder, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Advance
        final Collection<KeyStatusPair<K, V, Ctx>> dependencies = nextStatus.getDependencies(holder);
        holder.setDependencies(nextStatus, dependencies);
        final CompletableFuture<Void> dependencyFuture = getDependencyFuture0(dependencies, key);
        holder.submitOp(dependencyFuture.thenCompose(unused -> {
            final Ctx ctx = makeContext(holder, nextStatus, false);
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

    private CompletableFuture<Void> getDependencyFuture0(Collection<KeyStatusPair<K, V, Ctx>> dependencies, K key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicInteger satisfied = new AtomicInteger(0);
        final int size = dependencies.size();
        if (size == 0) {
            return CompletableFuture.completedFuture(null);
        }
        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
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

    public void addTicket(K pos, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        this.getExecutor().execute(() -> this.addTicketWithSource(pos, pos, targetStatus, callback));
    }

    private void addTicketWithSource(K pos, K source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        if (this.getUnloadedStatus().equals(targetStatus)) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        ItemHolder<K, V, Ctx> holder = this.items.computeIfAbsent(pos, (K k) -> {
            final ItemHolder<K, V, Ctx> holder1 = new ItemHolder<>(this.getUnloadedStatus(), k);
            this.onItemCreation(holder1);
            return holder1;
        });
        holder.addTicket(new ItemTicket<>(source, targetStatus, callback));
        markDirty(pos);
    }

    public void removeTicket(K pos, ItemStatus<K, V, Ctx> targetStatus) {
        this.getExecutor().execute(() -> this.removeTicketWithSource(pos, pos, targetStatus));
    }

    private void removeTicketWithSource(K pos, K source, ItemStatus<K, V, Ctx> targetStatus) {
        ItemHolder<K, V, Ctx> holder = this.items.get(pos);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.removeTicket(new ItemTicket<>(source, targetStatus, null));
        markDirty(pos);
    }

    private ItemStatus<K, V, Ctx> getNextStatus(ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> target) {
        Assertions.assertTrue(target != null);
        final int compare = Integer.compare(current.ordinal(), target.ordinal());
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
