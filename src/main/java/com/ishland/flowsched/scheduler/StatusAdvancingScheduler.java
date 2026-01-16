package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;

import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

/**
 * A scheduler that advances status of items.
 *
 * @param <K> the key type
 * @param <V> the item type
 * @param <Ctx> the context type
 */
public abstract class StatusAdvancingScheduler<K, V, Ctx, UserData> {

    public static final Runnable NO_OP = () -> {
    };

    private final StampedLock itemsLock = new StampedLock();
    private final Object2ReferenceOpenHashMap<K, ItemHolder<K, V, Ctx, UserData>> items = new Object2ReferenceOpenHashMap<>() {
        @Override
        protected void rehash(int newN) {
            if (n < newN) {
                super.rehash(newN);
            }
        }
    };
    private final ObjectFactory objectFactory;

    protected StatusAdvancingScheduler() {
        this(new ObjectFactory.DefaultObjectFactory());
    }

    protected StatusAdvancingScheduler(ObjectFactory objectFactory) {
        this.objectFactory = Objects.requireNonNull(objectFactory);
    }

    protected abstract Executor getBackgroundExecutor();

    protected Scheduler getSchedulerBackedByBackgroundExecutor() {
        return Schedulers.from(getBackgroundExecutor());
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

    void tickHolder0(ItemHolder<K, V, Ctx, UserData> holder) {
        final K key = holder.getKey();
        if (getHolder(key) != holder) return;
//        holder.sanitizeSetStatus = Thread.currentThread();
        if (holder.isBusy()) {
            tickHandleBusy0(holder);
            return;
        }

        final ItemStatus<K, V, Ctx> current;
        ItemStatus<K, V, Ctx> nextStatus;
        synchronized (holder) {
            if (holder.isBusy()) {
                holder.executeCriticalSectionAndBusy(() -> this.tickHandleBusy0(holder));
                return;
            }
            current = holder.getStatus();
            nextStatus = getNextStatus(current, holder.getTargetStatus());
            Assertions.assertTrue(holder.getStatus() == current);
            holder.validateCompletedFutures(current);
//        holder.sanitizeSetStatus = null;
            if (nextStatus == current) {
                holder.flushUnloadedStatus(current);
                holder.validateAllFutures();
                if (current.equals(getUnloadedStatus())) {
                    if (holder.isDependencyDirty()) {
                        holder.executeCriticalSectionAndBusy(() -> holder.cleanupDependencies(this));
                        holder.markDirty(this);
                        return;
                    }
                    if (holder.holdsDependency()) {
                        if (holder.isDependencyDirty()) {
                            holder.markDirty(this); // should rarely happen
                            return;
                        }
                        System.err.println(String.format("BUG: %s still holds some dependencies when ready for unloading", holder.getKey()));
                    }
//                    System.out.println("Unloaded: " + key);
                    this.onItemRemoval(holder);
                    holder.release();
                    final long lock = this.itemsLock.writeLock();
                    try {
                        this.items.remove(key);
                    } finally {
                        this.itemsLock.unlockWrite(lock);
                    }
                    return;
                }
                holder.executeCriticalSectionAndBusy(() -> holder.cleanupDependencies(this));
                return;
            }
        }

        Assertions.assertTrue(holder.getStatus() == current);
        if (current.ordinal() < nextStatus.ordinal()) {
            if ((holder.getFlags() & ItemHolder.FLAG_BROKEN) != 0) {
                return;
            }
//            holder.submitOp(CompletableFuture.runAsync(() -> advanceStatus0(holder, nextStatus, key), getBackgroundExecutor()));
            Assertions.assertTrue(holder.getStatus() == current);
            holder.busyRefCounter().incrementRefCount();
            try {
                advanceStatus0(holder, nextStatus, key);
            } finally {
                holder.busyRefCounter().decrementRefCount();
            }
        } else {
            holder.busyRefCounter().incrementRefCount();
            try {
                downgradeStatus0(holder, current, nextStatus, key);
            } finally {
                holder.busyRefCounter().decrementRefCount();
            }
        }
    }

    private void tickHandleBusy0(ItemHolder<K, V, Ctx, UserData> holder) {
        // this is not protected by any synchronization, data can be unstable here
        final ItemStatus<K, V, Ctx> upgradingStatusTo = holder.upgradingStatusTo();
        final ItemStatus<K, V, Ctx> current = holder.getStatus();
        ItemStatus<K, V, Ctx> nextStatus = getNextStatus(current, holder.getTargetStatus());
        ItemStatus<K, V, Ctx> projectedCurrent = upgradingStatusTo != null ? upgradingStatusTo : current;
        if (projectedCurrent.ordinal() > nextStatus.ordinal()) {
            holder.tryCancelUpgradeAction();
        }
        holder.consolidateMarkDirty(this);
        return;
    }

    private void downgradeStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> nextStatus, K key) {
        // Downgrade
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(current);
        Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");

        Cancellable cancellable = new Cancellable();

        AtomicReference<Ctx> contextRef = new AtomicReference<>(null);
        AtomicBoolean hasDowngraded = new AtomicBoolean(false);

        final Completable completable = Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, current, dependencies, false);
                    Assertions.assertTrue(ctx != null);
                    contextRef.set(ctx);
                    final Completable stage = current.preDowngradeFromThis(ctx, cancellable);
                    return stage;
                })
                .andThen(Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());

                    final boolean success = holder.setStatus(nextStatus, false);
                    Assertions.assertTrue(success, "setStatus on downgrade failed");

                    hasDowngraded.set(true);

                    final Ctx ctx = contextRef.get();
                    Objects.requireNonNull(ctx);
                    final Completable stage = current.downgradeFromThis(ctx, cancellable);
                    return stage.cache();
                }))
                .doOnEvent((throwable) -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (cancellable.isCancelled() && actual instanceof CancellationException) {
                                if (hasDowngraded.get()) {
                                    holder.setStatus(current, true);
                                }
                                holder.consolidateMarkDirty(this);
                                return;
                            }
                        }

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
                        holder.consolidateMarkDirty(this);
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
        Cancellable cancellable = new Cancellable();

        CancellationSignaller signaller = new CancellationSignaller(unused -> {
            cancellable.cancel();
            dependencyCompletable.cancel();
        });

        AtomicReference<Ctx> contextRef = new AtomicReference<>(null);

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
                    Assertions.assertTrue(ctx != null);
                    contextRef.set(ctx);
                    final Completable stage = nextStatus.upgradeToThis(ctx, cancellable);
                    return stage.cache();
                }))
                .onErrorResumeNext(throwable -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (cancellable.isCancelled() && actual instanceof CancellationException) {
                                if (holder.getDependencies(nextStatus) != null) {
                                    releaseDependencies(holder, nextStatus);
                                }
                                try {
                                    signaller.fireComplete(actual);
                                } catch (Throwable t) {
                                    t.printStackTrace();
                                }
                                holder.consolidateMarkDirty(this);
                                return Completable.error(throwable);
                            }
                        }

                        Assertions.assertTrue(holder.getDependencies(nextStatus) != null);

                        final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, true, throwable);
                        switch (action) {
                            case PROCEED -> {
                                return Completable.complete();
                            }
                            case MARK_BROKEN -> {
                                holder.setFlag(ItemHolder.FLAG_BROKEN);
                                clearDependencies0(holder, nextStatus);
                                holder.consolidateMarkDirty(this);
                                return Completable.error(throwable);
                            }
                            default -> {
                                throw new IllegalStateException("Unexpected value: " + action);
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        if (throwable != null) {
                            throwable.addSuppressed(t);
                            return Completable.error(throwable);
                        } else {
                            return Completable.error(t);
                        }
                    }
                })
                .doOnEvent(throwable -> {
                    try {
                        if (throwable == null) {
                            holder.setStatus(nextStatus, false);
                            rerequestDependencies(holder, nextStatus);
                            holder.consolidateMarkDirty(this);
                            onItemUpgrade(holder, nextStatus);
                        }

                        try {
                            signaller.fireComplete(null);
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    } catch (Throwable t) {
                        try {
                            holder.setFlag(ItemHolder.FLAG_BROKEN);
                            clearDependencies0(holder, nextStatus);
                            holder.consolidateMarkDirty(this);
                        } catch (Throwable t1) {
                            t.addSuppressed(t1);
                        }
                        t.printStackTrace();
                    }
                })
                .andThen(
                        Completable
                                .defer(() -> {
                                    Ctx ctx = contextRef.get();
                                    Assertions.assertTrue(ctx != null);
                                    return nextStatus.postUpgradeToThis(ctx).cache();
                                })
                                .onErrorResumeNext(throwable -> {
                                    final ExceptionHandlingAction action = this.tryHandleTransactionException(holder, nextStatus, true, throwable);
                                    switch (action) {
                                        case PROCEED -> {
                                            return Completable.complete();
                                        }
                                        case MARK_BROKEN -> {
                                            holder.setFlag(ItemHolder.FLAG_BROKEN);
                                            holder.consolidateMarkDirty(this);
                                            holder.executeCriticalSectionAndBusy(() -> {
                                                holder.busyRefCounter().incrementRefCount();
                                                try {
                                                    downgradeStatus0(holder, nextStatus, nextStatus.getPrev(), key);
                                                } finally {
                                                    holder.busyRefCounter().decrementRefCount();
                                                }
                                            });
                                            return Completable.error(throwable);
                                        }
                                        default -> {
                                            throw new IllegalStateException("Unexpected value: " + action);
                                        }
                                    }
                                })
                )
                .onErrorComplete()
                .cache();

        holder.submitUpgradeAction(signaller, nextStatus);
        holder.subscribeOp(completable);
        completable.subscribe(() -> signaller.fireComplete(null), signaller::fireComplete);
        Assertions.assertTrue(holder.isBusy() || (cancellable.isCancelled() || holder.getStatus() == nextStatus));
    }

    private void rerequestDependencies(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> status) { // sync externally
        final KeyStatusPair<K, V, Ctx>[] curDep = holder.getDependencies(status);
        final KeyStatusPair<K, V, Ctx>[] newDep = status.getDependencies(holder);
        final KeyStatusPair<K, V, Ctx>[] toAdd = status.getDependenciesToAdd(holder);
        final KeyStatusPair<K, V, Ctx>[] toRemove = status.getDependenciesToRemove(holder);
        holder.setDependencies(status, null);
        holder.setDependencies(status, newDep);
        for (KeyStatusPair<K, V, Ctx> pair : toAdd) {
            holder.addDependencyTicket(this, pair.key(), pair.status(), null);
        }
        for (KeyStatusPair<K, V, Ctx> pair : toRemove) {
            holder.removeDependencyTicket(pair.key(), pair.status());
        }
    }

    public ItemHolder<K, V, Ctx, UserData> getHolder(K key) {
        long stamp = this.itemsLock.tryOptimisticRead();
        if (stamp != 0L) {
            try {
                ItemHolder<K, V, Ctx, UserData> holder = this.items.get(key);
                if (this.itemsLock.validate(stamp)) {
                    return holder;
                }
                // fall through
            } catch (Throwable ignored) {
                // fall through
            }
        }

        stamp = this.itemsLock.readLock();
        try {
            return this.items.get(key);
        } finally {
            this.itemsLock.unlockRead(stamp);
        }
    }

    private ItemHolder<K, V, Ctx, UserData> getOrCreateHolder(K key) {
        long stamp = this.itemsLock.tryOptimisticRead();
        ItemHolder<K, V, Ctx, UserData> holder;
        boolean tryReadAgain = true;
        if (stamp != 0L) {
            try {
                holder = this.items.get(key);
                if (this.itemsLock.validate(stamp)) {
                    tryReadAgain = false;
                    if (holder != null) {
                        return holder;
                    }
                }
                // fall through
            } catch (Throwable ignored) {
                // fall through
            }
        }

        long writeStamp;
        if (tryReadAgain) {
            stamp = this.itemsLock.readLock();
            try {
                holder = this.items.get(key);
            } catch (Throwable t) {
                t.printStackTrace();
                this.itemsLock.unlockRead(stamp);
                throw t;
            }
            if (holder != null) {
                this.itemsLock.unlockRead(stamp);
                return holder;
            }
            holder = createHolder0(key); // move creation out of write lock region
            writeStamp = this.itemsLock.tryConvertToWriteLock(stamp);
            if (writeStamp == 0L) {
                this.itemsLock.unlockRead(stamp);
                writeStamp = this.itemsLock.writeLock();
            }
        } else {
            holder = createHolder0(key); // move creation out of write lock region
            writeStamp = this.itemsLock.writeLock();
        }
        try {
            ItemHolder<K, V, Ctx, UserData> inMap = this.items.putIfAbsent(key, holder);
            if (inMap == null) { // put successfully
                this.onItemCreation(holder);
            } else {
                holder = inMap; // return the correct thing
            }
        } finally {
            this.itemsLock.unlockWrite(writeStamp);
        }
        return holder;
    }

    public int itemCount() {
        VarHandle.acquireFence();
        return this.items.size();
    }

    protected void wakeUp() {
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
            if (satisfied.get() == 0);
            if (finished.compareAndSet(false, true)) {
                releaseDependencies(holder, nextStatus);
                signaller1.fireComplete(new CancellationException());
            }
        });
        try {
            Runnable callback = () -> {
                final int incrementAndGet = satisfied.incrementAndGet();
                Assertions.assertTrue(incrementAndGet <= size, "Satisfied more than expected");
                if (incrementAndGet == size) {
                    if (finished.compareAndSet(false, true)) {
                        holder.getCriticalSectionExecutor().execute(() -> signaller.fireComplete(null));
                    }
                }
            };
            for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
                Assertions.assertTrue(!dependency.key().equals(holder.getKey()));
                holder.addDependencyTicket(this, dependency.key(), dependency.status(), callback);
            }
        } catch (Throwable t) {
            signaller.fireComplete(t);
        }
        return signaller;
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, key, targetStatus, callback);
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, ItemTicket.TicketType.EXTERNAL, source, targetStatus, callback);
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, new ItemTicket<>(type, source, targetStatus, callback));
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemTicket<K, V, Ctx> ticket) {
        if (this.getUnloadedStatus().equals(ticket.getTargetStatus())) {
            throw new IllegalArgumentException("Cannot add ticket to unloaded status");
        }
        try {
            while (true) {
                ItemHolder<K, V, Ctx, UserData> holder = this.getOrCreateHolder(key);

                synchronized (holder) {
                    if (!holder.isOpen()) {
                        // holder got removed before we had chance to add a ticket to it, retry
//                        System.out.println(String.format("Retrying addTicket0(%s, %s)", key, ticket));
                        continue;
                    }
                    holder.busyRefCounter().incrementRefCount();
                }
                try {
                    holder.addTicket(ticket);
                    holder.consolidateMarkDirty(this);
                } finally {
                    holder.busyRefCounter().decrementRefCount();
                }
                return holder;
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }

    private ItemHolder<K, V, Ctx, UserData> createHolder0(K k) {
        return new ItemHolder<>(this.getUnloadedStatus(), k, this.objectFactory, this.getBackgroundExecutor());
    }

    public void removeTicket(K key, ItemStatus<K, V, Ctx> targetStatus) {
        this.removeTicket(key, ItemTicket.TicketType.EXTERNAL, key, targetStatus);
    }

    public void removeTicket(K key, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus) {
        ItemHolder<K, V, Ctx, UserData> holder = this.getHolder(key);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.removeTicket(new ItemTicket<>(type, source, targetStatus, null));
        // holder may have been removed at this point, only mark it dirty if it still exists
        holder.tryMarkDirty(this);
    }

    public void swapTicket(K key, ItemTicket<K, V, Ctx> orig, ItemTicket<K, V, Ctx> ticket) {
        ItemHolder<K, V, Ctx, UserData> holder = this.getHolder(key);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.swapTicket(orig, ticket);
        holder.markDirty(this);
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

    private void clearDependencies0(final ItemHolder<K, V, Ctx, UserData> holder, final ItemStatus<K, V, Ctx> fromStatus) { // sync externally
        for (int i = fromStatus.ordinal(); i > 0; i--) {
            final ItemStatus<K, V, Ctx> status = this.getUnloadedStatus().getAllStatuses()[i];
            this.releaseDependencies(holder, status);
            holder.setDependencies(status, new KeyStatusPair[0]);
        }
    }

    private void releaseDependencies(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> status) {
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(status);
        for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
            holder.removeDependencyTicket(dependency.key(), dependency.status());
        }
        holder.setDependencies(status, null);
    }

}
