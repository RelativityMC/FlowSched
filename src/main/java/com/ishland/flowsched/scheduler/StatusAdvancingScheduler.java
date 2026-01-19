package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
        Cancellable cancellable;
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
            if (current.ordinal() < nextStatus.ordinal() && (holder.getFlags() & ItemHolder.FLAG_BROKEN) != 0) {
                return;
            }
            cancellable = new Cancellable();
            holder.submitAction(cancellable, nextStatus);
            holder.busyRefCounter().incrementRefCount();
        }

        try {
            Assertions.assertTrue(holder.getStatus() == current);
            if (current.ordinal() < nextStatus.ordinal()) {
                advanceStatus0(holder, nextStatus, key, cancellable);
            } else {
                downgradeStatus0(holder, current, nextStatus, key, cancellable);
            }
        } finally {
            holder.busyRefCounter().decrementRefCount();
        }
    }

    private void tickHandleBusy0(ItemHolder<K, V, Ctx, UserData> holder) {
        // this is not protected by any synchronization, data can be unstable here
        final ItemStatus<K, V, Ctx> statusTo = holder.changingStatusTo();
        final ItemStatus<K, V, Ctx> current = holder.getStatus();
        ItemStatus<K, V, Ctx> nextStatus = getNextStatus(current, holder.getTargetStatus());
        ItemStatus<K, V, Ctx> projectedCurrent = statusTo != null ? statusTo : current;
        if (nextStatus != projectedCurrent) {
            holder.tryCancelAction();
        }
        holder.consolidateMarkDirty(this);
    }

    private void downgradeStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> current, ItemStatus<K, V, Ctx> nextStatus, K key, Cancellable cancellable) {
        // Downgrade
        final KeyStatusPair<K, V, Ctx>[] dependencies = holder.getDependencies(current);
        Assertions.assertTrue(dependencies != null, "No dependencies for downgrade");

        AtomicReference<Ctx> contextRef = new AtomicReference<>(null);
        AtomicBoolean hasDowngraded = new AtomicBoolean(false);

        final Completable completable = Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, current, dependencies, false);
                    Assertions.assertTrue(ctx != null);
                    contextRef.setPlain(ctx);
                    final Completable stage = current.preDowngradeFromThis(ctx, cancellable);
                    return stage;
                })
                .andThen(Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());

                    final boolean success = holder.setStatus(nextStatus, false);
                    Assertions.assertTrue(success, "setStatus on downgrade failed");

                    hasDowngraded.setPlain(true);

                    final Ctx ctx = contextRef.getPlain();
                    Objects.requireNonNull(ctx);
                    final Completable stage = current.downgradeFromThis(ctx, cancellable);
                    return stage.cache();
                }))
                .doOnEvent(unused -> holder.finishAction())
                .doOnEvent((throwable) -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (cancellable.isCancelled() && actual instanceof CancellationException) {
                                if (hasDowngraded.getPlain()) {
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

    private void advanceStatus0(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, K key, Cancellable cancellation) {
        // Advance
        final KeyStatusPair<K, V, Ctx>[] dependencies = nextStatus.getDependencies(holder);
        Cancellable upgradeCancellable = new Cancellable();
        Cancellable depCancellable = new Cancellable();

        final boolean success = cancellation.setup(() -> {
            upgradeCancellable.cancel();
            depCancellable.cancel();
        });

        if (!success) {
            upgradeCancellable.cancel();
            depCancellable.cancel();
            holder.finishAction();
            Assertions.assertTrue(false, "Raced early cancel?");
            return;
        }

        AtomicReference<Ctx> contextRef = new AtomicReference<>(null);

        final Completable completable = getDependencyFuture0(dependencies, holder, nextStatus, depCancellable)
                .andThen(Completable.defer(() -> {
                    Assertions.assertTrue(holder.isBusy());
                    final Ctx ctx = makeContext(holder, nextStatus, dependencies, false);
                    Assertions.assertTrue(ctx != null);
                    contextRef.setPlain(ctx);
                    return nextStatus.upgradeToThis(ctx, upgradeCancellable).cache();
                }))
                .onErrorResumeNext(throwable -> {
                    try {
                        Assertions.assertTrue(holder.isBusy());

                        {
                            Throwable actual = throwable;
                            while (actual instanceof CompletionException ex) actual = ex.getCause();
                            if (upgradeCancellable.isCancelled() && actual instanceof CancellationException) {
                                if (holder.getDependencies(nextStatus) != null) {
                                    releaseDependencies(holder, nextStatus);
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
                .doOnEvent(unused -> holder.finishAction())
                .andThen(
                        Completable
                                .defer(() -> {
                                    Ctx ctx = contextRef.getPlain();
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
                                                Cancellable cancellation1 = new Cancellable();
                                                // TODO: better broken downgrade handling
                                                cancellation1.complete();
                                                holder.submitAction(cancellation1, nextStatus.getPrev());
                                                try {
                                                    downgradeStatus0(holder, nextStatus, nextStatus.getPrev(), key, cancellation1);
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
        holder.subscribeOp(completable);
        Assertions.assertTrue(holder.isBusy() || (upgradeCancellable.isCancelled() || holder.getStatus() == nextStatus));
    }

    private void rerequestDependencies(ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> status) { // sync externally
        final KeyStatusPair<K, V, Ctx>[] curDep = holder.getDependencies(status);
        final KeyStatusPair<K, V, Ctx>[] newDep = status.getDependencies(holder);
        final KeyStatusPair<K, V, Ctx>[] toAdd = status.getDependenciesToAdd(holder);
        final KeyStatusPair<K, V, Ctx>[] toRemove = status.getDependenciesToRemove(holder);
        holder.setDependencies(status, null);
        holder.setDependencies(status, newDep);
        if (toAdd.length > 0) {
            ItemTicket ticket = new ItemTicket(ItemTicket.TicketType.DEPENDENCY, holder.getKey(), null, toAdd.length);
            for (KeyStatusPair<K, V, Ctx> pair : toAdd) {
                holder.addDependencyTicket(this, pair.key(), pair.status(), ticket);
            }
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
        this.itemsLock.tryOptimisticRead();
        return this.items.size();
    }

    protected void wakeUp() {
    }

    private Completable getDependencyFuture0(KeyStatusPair<K, V, Ctx>[] dependencies, ItemHolder<K, V, Ctx, UserData> holder, ItemStatus<K, V, Ctx> nextStatus, Cancellable cancellable) {
        final int size = dependencies.length;
        if (size == 0 && cancellable.setup(NO_OP)) {
            cancellable.complete();
            holder.setDependencies(nextStatus, dependencies);
            return Completable.complete();
        }

        return Completable.create(emitter -> {
            AtomicBoolean finished = new AtomicBoolean(false);
            holder.setDependencies(nextStatus, dependencies);
            final boolean success = cancellable.setup(() -> {
                if (finished.compareAndSet(false, true)) {
                    releaseDependencies(holder, nextStatus);
                    emitter.onError(new CancellationException());
                }
            });
            Assertions.assertTrue(success, "Raced early cancel?");
            try {
                Runnable callback = () -> {
                    if (finished.compareAndSet(false, true)) {
                        cancellable.complete();
                        holder.getCriticalSectionExecutor().execute(emitter::onComplete);
                    }
                };
                ItemTicket ticket = new ItemTicket(ItemTicket.TicketType.DEPENDENCY, holder.getKey(), callback, dependencies.length);
                for (KeyStatusPair<K, V, Ctx> dependency : dependencies) {
                    Assertions.assertTrue(!dependency.key().equals(holder.getKey()));
                    holder.addDependencyTicket(this, dependency.key(), dependency.status(), ticket);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                if (finished.compareAndSet(false, true)) {
                    releaseDependencies(holder, nextStatus);
                    emitter.onError(t);
                }
            }
        });
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, key, targetStatus, callback);
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket(key, ItemTicket.TicketType.EXTERNAL, source, targetStatus, callback);
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket(K key, ItemTicket.TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        return this.addTicket0(key, targetStatus, new ItemTicket(type, source, callback));
    }

    public ItemHolder<K, V, Ctx, UserData> addTicket0(K key, ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(targetStatus);
        Objects.requireNonNull(ticket);
        if (this.getUnloadedStatus().equals(targetStatus)) {
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
                    holder.addTicket(targetStatus, ticket);
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
        this.removeTicket0(key, targetStatus, new ItemTicket(type, source, null));
    }

    public void removeTicket0(K key, ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(targetStatus);
        Objects.requireNonNull(ticket);
        ItemHolder<K, V, Ctx, UserData> holder = this.getHolder(key);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.removeTicket(targetStatus, ticket);
        // holder may have been removed at this point, only mark it dirty if it still exists
        holder.tryMarkDirty(this);
    }

    public void swapTicket(K key, ItemStatus<K, V, Ctx> origStatus, ItemTicket orig, ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        Objects.requireNonNull(origStatus);
        Objects.requireNonNull(orig);
        Objects.requireNonNull(targetStatus);
        Objects.requireNonNull(ticket);
        ItemHolder<K, V, Ctx, UserData> holder = this.getHolder(key);
        if (holder == null) {
            throw new IllegalStateException("No such item");
        }
        holder.swapTicket(origStatus, orig, targetStatus, ticket);
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
