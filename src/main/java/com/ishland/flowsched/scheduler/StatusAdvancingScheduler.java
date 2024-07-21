package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.structs.SimpleObjectPool;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMaps;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSets;

import java.lang.invoke.VarHandle;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final SimpleObjectPool<TicketSet<K, V, Ctx>> ticketSetPool = new SimpleObjectPool<>(
            unused -> new TicketSet<>(getUnloadedStatus()),
            TicketSet::clear,
            TicketSet::clear,
            4096
    );

    protected abstract Executor getExecutor();

    protected Scheduler getSchedulerBackedByExecutor() {
        return Schedulers.from(getExecutor());
    }

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

    /**
     * Called when an item is deleted.
     *
     * @implNote This method is called when the monitor of the holder is held.
     * @param holder
     */
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
            synchronized (holder) {
                final ItemStatus<K, V, Ctx> current = holder.getStatus();
                ItemStatus<K, V, Ctx> nextStatus = getNextStatus(current, holder.getTargetStatus());
                if (holder.isBusy()) {
                    ItemStatus<K, V, Ctx> projectedCurrent = holder.isUpgrading() ? current.getNext() : current;
                    if (projectedCurrent.ordinal() > nextStatus.ordinal()) {
                        getExecutor().execute(holder::tryCancelUpgradeAction);
                    }
                    holder.submitOpListener(() -> markDirty(key));
                    continue;
                }
                if (nextStatus == current) {
                    if (current.equals(getUnloadedStatus())) {
//                    System.out.println("Unloaded: " + key);
                        this.onItemRemoval(holder);
                        holder.release(ticketSetPool);
                        this.items.remove(key);
                    }
                    continue; // No need to update
                }
                if (current.ordinal() < nextStatus.ordinal()) {
                    if ((holder.getFlags() & ItemHolder.FLAG_BROKEN) != 0) {
                        continue; // not allowed to upgrade
                    }
                    holder.submitOp(CompletableFuture.runAsync(() -> advanceStatus0(holder, nextStatus, key), getExecutor()));
                } else {
                    holder.setStatus(nextStatus);
                    holder.submitOp(CompletableFuture.runAsync(() -> downgradeStatus0(holder, current, nextStatus, key), getExecutor()));
                }
            }
        }
        return hasWork;
    }

    private void downgradeStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Downgrade
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(current);
        Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");

        final Completable completable = Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, current, dependencies, false);
                    final CompletionStage<Void> stage = current.downgradeFromThis(ctx);
                    return Completable.fromCompletionStage(stage);
                })
                .observeOn(getSchedulerBackedByExecutor())
                .doOnEvent((throwable) -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, false, throwable);
                        switch (action) {
                            case PROCEED -> {
                                releaseDependencies(holder, current);
                            }
                            case MARK_BROKEN -> {
                                holder.setFlag(ItemHolder.FLAG_BROKEN);
                                clearDependencies0(holder, current);
                            }
                        }
                        markDirty(key);
                        onItemDowngrade(holder, nextStatus);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                });
        holder.subscribeOp(completable);
    }

    private void advanceStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Advance
        final KeyStatusPair<K, V, Ctx>[] dependencies = nextStatus.getDependencies(holder);
        final CancellationSignaller dependencyCompletable = getDependencyFuture0(dependencies, holder, nextStatus);
        AtomicBoolean isCancelled = new AtomicBoolean(false);

        final Completable completable = Completable.create(emitter -> dependencyCompletable.addListener(throwable -> {
                    if (throwable != null) {
                        emitter.onError(throwable);
                    } else {
                        emitter.onComplete();
                    }
                }))
                .andThen(Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, nextStatus, dependencies, false);
                    final CompletionStage<Void> stage = nextStatus.upgradeToThis(ctx);
                    return Completable.fromCompletionStage(stage).cache();
                }))
                .observeOn(getSchedulerBackedByExecutor())
                .doOnEvent(throwable -> {
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

                        Assertions.assertTrue(holder.getDependencies(nextStatus) != null);

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
                        try {
                            holder.setFlag(ItemHolder.FLAG_BROKEN);
                            clearDependencies0(holder, nextStatus);
                            markDirty(key);
                        } catch (Throwable t1) {
                            t.addSuppressed(t1);
                        }
                        t.printStackTrace();
                    }
                })
                .onErrorComplete()
                .cache();

        CancellationSignaller signaller = new CancellationSignaller(unused -> {
            isCancelled.set(true);
            dependencyCompletable.cancel();
        });
        holder.submitUpgradeAction(signaller);
        holder.subscribeOp(completable);
        completable.subscribe(() -> signaller.fireComplete(null), signaller::fireComplete);
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

    private CancellationSignaller getDependencyFuture0(KeyStatusPair<K, V, Ctx>[] dependencies, ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus) {
        AtomicInteger satisfied = new AtomicInteger(0);
        final int size = dependencies.length;
        holder.setDependencies(nextStatus, dependencies);
        if (size == 0) {
            return CancellationSignaller.COMPLETED;
        }

        AtomicBoolean finished = new AtomicBoolean(false);
        final CancellationSignaller signaller = new CancellationSignaller(signaller1 -> {
            if (finished.compareAndSet(false, true)) {
                releaseDependencies(holder, nextStatus);
                signaller1.fireComplete(new CancellationException());
            }
        });
        final KeyStatusPair<K, V, Ctx> keyStatusPair = new KeyStatusPair<>(holder.getKey(), nextStatus);
        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
            Assertions.assertTrue(!dependency.key().equals(holder.getKey()));
            this.addTicketWithSource(dependency.key(), ItemTicket.TicketType.DEPENDENCY, keyStatusPair, dependency.status(), () -> {
                Assertions.assertTrue(this.getHolder(dependency.key()).getStatus().ordinal() >= dependency.status().ordinal());
                final int incrementAndGet = satisfied.incrementAndGet();
                Assertions.assertTrue(incrementAndGet <= size, "Satisfied more than expected");
                if (incrementAndGet == size) {
                    if (finished.compareAndSet(false, true)) {
                        signaller.fireComplete(null);
                    }
                }
            });
        }
        return signaller;
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K pos, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicketWithSource(pos, ItemTicket.TicketType.EXTERNAL, pos, targetStatus, callback);
    }

    private ItemHolder<K, V, Ctx, UserData> addTicketWithSource(K pos, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        if (this.getUnloadedStatus().equals(targetStatus)) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        while (true) {
            ItemHolder<K, V, Ctx, UserData> holder = this.items.computeIfAbsent(pos, (K k) -> {
                final ItemHolder<K, V, Ctx, UserData> holder1 = new ItemHolder<>(this.getUnloadedStatus(), k, ticketSetPool);
                this.onItemCreation(holder1);
                VarHandle.fullFence();
                return holder1;
            });
            synchronized (holder) {
                if ((holder.getFlags() & ItemHolder.FLAG_REMOVED) != 0) {
                    // holder got removed before we had chance to add a ticket to it, retry
                    continue;
                }
                holder.addTicket(new ItemTicket<>(type, source, targetStatus, callback));
            }
            markDirty(pos);
            return holder;
        }
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
