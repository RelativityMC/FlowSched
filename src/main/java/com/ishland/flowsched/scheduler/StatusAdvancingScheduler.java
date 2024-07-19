package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSets;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final ObjectSortedSet<K> pendingUpdates = ObjectSortedSets.synchronize(pendingUpdatesInternal, pendingUpdatesInternal);

    protected abstract Executor getExecutor();

    protected abstract ItemStatus<K, V, Ctx> getUnloadedStatus();

    protected abstract Ctx makeContext(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, KeyStatusPair<K, V, Ctx>[] dependencies, boolean isUpgrade);

    protected ExceptionHandlingAction handleTransactionException(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, boolean isUpgrade, Throwable throwable) {
        throwable.printStackTrace();
        return ExceptionHandlingAction.MARK_BROKEN;
    }

    protected void handleUnrecoverableException(Throwable throwable) {
    }

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
            synchronized (this.pendingUpdatesInternal) {
                key = this.pendingUpdatesInternal.removeFirst();
            }
            ItemHolder<K, V, Ctx, UserData> holder = this.items.get(key);
            if (holder == null) {
                continue;
            }
            final ItemStatus<K, V, Ctx> current = holder.getStatus();
            ItemStatus<K, V, Ctx> nextStatus = getNextStatus(current, holder.getTargetStatus());
            if (holder.isBusy()) {
                ItemStatus<K, V, Ctx> projectedCurrent = holder.isUpgrading() ? current.getNext() : current;
                if (projectedCurrent.ordinal() > nextStatus.ordinal()) {
                    holder.tryCancelUpgradeAction(); // cancel upgrade
                }
                holder.getOpFuture().whenComplete((unused, throwable) -> markDirty(key));
                continue;
            }
            if (nextStatus == current) {
                if (current.equals(getUnloadedStatus())) {
//                    System.out.println("Unloaded: " + key);
                    this.onItemRemoval(holder);
                    this.items.remove(key);
                }
                continue; // No need to update
            }
            if (current.ordinal() < nextStatus.ordinal()) {
                if ((holder.getFlags() & ItemHolder.FLAG_BROKEN) != 0) {
                    continue; // not allowed to upgrade
                }
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

        ArrayList<Runnable> delayedTasks = new ArrayList<>(1);
        holder.submitOp(CompletableFuture.supplyAsync(() -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, current, dependencies, false);
                    final CompletionStage<Void> stage = current.downgradeFromThis(ctx);
//                    stage.thenApply(Function.identity()).toCompletableFuture().orTimeout(60, TimeUnit.SECONDS).whenComplete((unused1, throwable1) -> {
//                        if (throwable1 instanceof TimeoutException) {
//                            System.out.println(String.format("Downgrading %s to %s is taking >60s", holder.getKey(), nextStatus));
//                        }
//                    });
                    return stage;
                }, delayedTasks::add)
                .thenCompose(Function.identity())
                .whenCompleteAsync((unused, throwable) -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, false, throwable);
                        switch (action) {
                            case PROCEED -> {
                                holder.setStatus(nextStatus);
                                releaseDependencies(holder, current);
                            }
                            case MARK_BROKEN -> {
                                holder.setFlag(ItemHolder.FLAG_BROKEN);
                                holder.setStatus(nextStatus);
                                clearDependencies0(holder, current);
                            }
                        }
                        markDirty(key);
                        onItemDowngrade(holder, nextStatus);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }, getExecutor()));
        for (Runnable task : delayedTasks) {
            task.run();
        }
    }

    private void advanceStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Advance
        final KeyStatusPair<K, V, Ctx>[] dependencies = nextStatus.getDependencies(holder);
        holder.setDependencies(nextStatus, dependencies);
        final CompletableFuture<Void> dependencyFuture = getDependencyFuture0(dependencies, key, nextStatus);
        AtomicBoolean isCancelled = new AtomicBoolean(false);

        final CompletableFuture<Void> future = dependencyFuture
                .thenComposeAsync(unused11 -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, nextStatus, dependencies, false);
                    final CompletionStage<Void> stage = nextStatus.upgradeToThis(ctx);
                    return stage;
                }, getExecutor())
                .whenCompleteAsync((unused, throwable) -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (isCancelled.get() && actual instanceof CancellationException) {
                                markDirty(key);
                                return;
                            }
                        }

                        final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, true, throwable);
                        switch (action) {
                            case PROCEED -> {
                                holder.setStatus(nextStatus);
                                markDirty(key);
                                onItemUpgrade(holder, nextStatus);
                            }
                            case MARK_BROKEN -> {
                                holder.setFlag(ItemHolder.FLAG_BROKEN);
                                clearDependencies0(holder, nextStatus);
                                markDirty(key);
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }, getExecutor());
        holder.submitOp(future);
        holder.submitUpgradeAction(new CancellationSignallingCompletableFuture<>(future, () -> {
            isCancelled.set(true);
            dependencyFuture.cancel(false);
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

        return new CancellationSignallingCompletableFuture<>(future, () -> {
            for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
                this.removeTicketWithSource(dependency.key(), ItemTicket.TicketType.DEPENDENCY, keyStatusPair, dependency.status());
            }
        });
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K pos, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicketWithSource(pos, ItemTicket.TicketType.EXTERNAL, pos, targetStatus, callback);
    }

    private ItemHolder<K, V, Ctx, UserData> addTicketWithSource(K pos, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        if (this.getUnloadedStatus().equals(targetStatus)) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        ItemHolder<K, V, Ctx, UserData> holder;
        synchronized (this.items) {
            holder = this.items.computeIfAbsent(pos, (K k) -> {
                final ItemHolder<K, V, Ctx, UserData> holder1 = new ItemHolder<>(this.getUnloadedStatus(), k);
                this.onItemCreation(holder1);
                VarHandle.fullFence();
                return holder1;
            });
            holder.addTicket(new ItemTicket<>(type, source, targetStatus, callback));
        }
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

    private ExceptionHandlingAction tryHandleTransactionException(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, boolean isUpgrade, Throwable throwable) {
        if (throwable == null) { // no exception to handle
            return ExceptionHandlingAction.PROCEED;
        }
        try {
            return this.handleTransactionException(holder, nextStatus, isUpgrade, throwable);
        } catch (Throwable t) {
            t.printStackTrace();
            return ExceptionHandlingAction.MARK_BROKEN;
        }
    }

    private void clearDependencies0(final ItemHolder<K, V, Ctx, UserData> holder, final ItemStatus<K, V, Ctx> fromStatus) {
        synchronized (holder) {
            for (int i = fromStatus.ordinal(); i > 0; i--) {
                final ItemStatus<K, V, Ctx> status = this.getUnloadedStatus().getAllStatuses()[i];
                this.releaseDependencies(holder, status);
                holder.setDependencies(status, new KeyStatusPair[0]);
            }
        }
    }

    private void releaseDependencies(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> status) {
        final KeyStatusPair<K, V, Ctx> keyStatusPair = new KeyStatusPair<>(holder.getKey(), status);
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(status);
        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
            this.removeTicketWithSource(dependency.key(), ItemTicket.TicketType.DEPENDENCY, keyStatusPair, dependency.status());
        }
        holder.setDependencies(status, null);
    }

    protected boolean hasPendingUpdates() {
        return !this.pendingUpdates.isEmpty();
    }

}
