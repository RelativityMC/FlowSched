package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSets;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A scheduler that advances status of items.
 *
 * @param <K> the key type
 * @param <V> the item type
 * @param <Ctx> the context type
 */
public abstract class StatusAdvancingScheduler<K, V, Ctx, UserData> {

    private final Object2ReferenceMap<K, ItemHolder<K, V, Ctx, UserData>> items = Object2ReferenceMaps.synchronize(new Object2ReferenceOpenHashMap<>());
    private final ObjectLinkedOpenHashSet<K> pendingUpdatesInternal = new ObjectLinkedOpenHashSet<>();
    private final ObjectSortedSet<K> pendingUpdates = ObjectSortedSets.synchronize(pendingUpdatesInternal);

    protected abstract Executor getExecutor();

    protected abstract ItemStatus<K, V, Ctx> getUnloadedStatus();

    protected abstract Ctx makeContext(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, KeyStatusPair<K, V, Ctx>[] dependencies, boolean isUpgrade);

    /**
     * Called when an item is created.
     *
     * @implNote This method is called before the item is added to the internal map. Make sure to not access the item from the map.
     *           May get called from any thread.
     * @param holder
     */
    protected void onItemCreation(ItemHolder<K, V, Ctx, UserData> holder) {
    }

    protected void onItemRemoval(ItemHolder<K, V, Ctx, UserData> holder) {
    }

    protected void onItemUpgrade(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> statusReached) {
    }

    protected void onItemDowngrade(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> statusReached) {
    }


    public boolean tick() {
        boolean hasWork = false;
        while (!this.pendingUpdates.isEmpty()) {
            hasWork = true;
            K key;
            synchronized (this.pendingUpdates) {
                key = this.pendingUpdatesInternal.removeFirst();
            }
            ItemHolder<K, V, Ctx, UserData> holder = this.items.get(key);
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

    private void downgradeStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Downgrade
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(current);
        Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");
        holder.setDependencies(current, null);
        final Ctx ctx = makeContext(holder, current, dependencies, false);
        holder.submitOp(CompletableFuture.supplyAsync(() -> current.downgradeFromThis(ctx), Runnable::run)
                .thenCompose(Function.identity())
                .whenCompleteAsync((unused, throwable) -> {
                    try {
                        // TODO exception handling
                        holder.setStatus(nextStatus);
                        final KeyStatusPair<K, V, Ctx> keyStatusPair = new KeyStatusPair<>(holder.getKey(), current);
                        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
                            this.removeTicketWithSource(dependency.key(), ItemTicket.TicketType.DEPENDENCY, keyStatusPair, dependency.status());
                        }
                        markDirty(key);
                        onItemDowngrade(holder, nextStatus);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }, getExecutor()));
    }

    private void advanceStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Advance
        final KeyStatusPair<K, V, Ctx>[] dependencies = nextStatus.getDependencies(holder);
        holder.setDependencies(nextStatus, dependencies);
        final CompletableFuture<Void> dependencyFuture = getDependencyFuture0(dependencies, key, nextStatus);
        holder.submitOp(dependencyFuture.thenCompose(unused -> {
            final Ctx ctx = makeContext(holder, nextStatus, dependencies, false);
            return nextStatus.upgradeToThis(ctx);
        }).whenCompleteAsync((unused, throwable) -> {
            try {
                // TODO exception handling
                holder.setStatus(nextStatus);
                markDirty(key);
                onItemUpgrade(holder, nextStatus);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, getExecutor()).whenComplete((unused, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            }
        }));
    }

    public ItemHolder<K, V, Ctx, UserData> getHolder(K key) {
        return this.items.get(key);
    }

    public int itemCount() {
        return this.items.size();
    }

    protected void markDirty(K key) {
        this.pendingUpdates.add(key);
    }

    private CompletableFuture<Void> getDependencyFuture0(KeyStatusPair<K, V, Ctx>[] dependencies, K key, ItemStatus<K, V, Ctx> nextStatus) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicInteger satisfied = new AtomicInteger(0);
        final int size = dependencies.length;
        if (size == 0) {
            return CompletableFuture.completedFuture(null);
        }
        final KeyStatusPair<K, V, Ctx> keyStatusPair = new KeyStatusPair<>(key, nextStatus);
        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
            Assertions.assertTrue(!dependency.key().equals(key));
            this.addTicketWithSource(dependency.key(), ItemTicket.TicketType.DEPENDENCY, keyStatusPair, dependency.status(), () -> {
                final int incrementAndGet = satisfied.incrementAndGet();
                Assertions.assertTrue(incrementAndGet <= size, "Satisfied more than expected");
                if (incrementAndGet == size) {
                    future.complete(null);
                }
            });
        }
        return future;
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K pos, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicketWithSource(pos, ItemTicket.TicketType.EXTERNAL, pos, targetStatus, callback);
    }

    private ItemHolder<K, V, Ctx, UserData> addTicketWithSource(K pos, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        if (this.getUnloadedStatus().equals(targetStatus)) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        ItemHolder<K, V, Ctx, UserData> holder = this.items.computeIfAbsent(pos, (K k) -> {
            final ItemHolder<K, V, Ctx, UserData> holder1 = new ItemHolder<>(this.getUnloadedStatus(), k);
            this.onItemCreation(holder1);
            return holder1;
        });
        holder.addTicket(new ItemTicket<>(type, source, targetStatus, callback));
        markDirty(pos);
        return holder;
    }

    public void removeTicket(K pos, ItemStatus<K, V, Ctx> targetStatus) {
        this.removeTicketWithSource(pos, ItemTicket.TicketType.EXTERNAL, pos, targetStatus);
    }

    private void removeTicketWithSource(K pos, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus) {
        ItemHolder<K, V, Ctx, UserData> holder = this.items.get(pos);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.removeTicket(new ItemTicket<>(type, source, targetStatus, null));
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
