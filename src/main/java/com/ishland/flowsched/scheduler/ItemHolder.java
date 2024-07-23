package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.structs.SimpleObjectPool;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceLists;

import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ItemHolder<K, V, Ctx, UserData> {

    public static final IllegalStateException UNLOADED_EXCEPTION = new IllegalStateException("Not loaded");
    private static final CompletableFuture<Void> UNLOADED_FUTURE = CompletableFuture.failedFuture(UNLOADED_EXCEPTION);

    @SuppressWarnings("PointlessBitwiseExpression")
    public static final int FLAG_REMOVED = 1 << 0;
    /**
     * Indicates the holder have been marked broken
     * If set, the holder:
     * - will not be allowed to be upgraded any further
     * - will still be allowed to be downgraded, but operations to it should be careful
     */
    public static final int FLAG_BROKEN = 1 << 1;
    /**
     * Indicates the holder have at least one failed transactions and proceeded to retry
     */
    public static final int FLAG_HAVE_RETRIED = 1 << 2;

    private final K key;
    private final ItemStatus<K, V, Ctx> unloadedStatus;
    private final AtomicReference<V> item = new AtomicReference<>();
    private final AtomicReference<UserData> userData = new AtomicReference<>();
    private final BusyRefCounter busyRefCounter = new BusyRefCounter();
    private final AtomicReference<CancellationSignaller> runningUpgradeAction = new AtomicReference<>();
    private final TicketSet<K, V, Ctx> tickets;
    private final AtomicReference<ItemStatus<K, V, Ctx>> status = new AtomicReference<>();
    private final List<Pair<ItemStatus<K, V, Ctx>, Long>> statusHistory = ReferenceLists.synchronize(new ReferenceArrayList<>());
    private final KeyStatusPair<K, V, Ctx>[][] requestedDependencies;
    private final CompletableFuture<Void>[] futures;
    private final AtomicInteger flags = new AtomicInteger(0);

    ItemHolder(ItemStatus<K, V, Ctx> initialStatus, K key, SimpleObjectPool<TicketSet<K, V, Ctx>> ticketSetPool) {
        this.unloadedStatus = Objects.requireNonNull(initialStatus);
        this.status.set(this.unloadedStatus);
        this.key = Objects.requireNonNull(key);
        this.tickets = ticketSetPool.alloc();

        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.futures = new CompletableFuture[allStatuses.length];
        this.requestedDependencies = new KeyStatusPair[allStatuses.length][];
        for (int i = 0, allStatusesLength = allStatuses.length; i < allStatusesLength; i++) {
            this.futures[i] = UNLOADED_FUTURE;
            this.requestedDependencies[i] = null;
        }
        VarHandle.fullFence();
    }

    private synchronized void createFutures() {
        assertOpen();
        final ItemStatus<K, V, Ctx> targetStatus = this.getTargetStatus();
        for (int i = this.unloadedStatus.ordinal() + 1; i <= targetStatus.ordinal(); i++) {
            this.futures[i] = this.futures[i] == UNLOADED_FUTURE ? new CompletableFuture<>() : this.futures[i];
        }
    }

    /**
     * Get the target status of this item.
     *
     * @return the target status of this item, or null if no ticket is present
     */
    public synchronized ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.tickets.getTargetStatus();
    }

    public synchronized ItemStatus<K, V, Ctx> getStatus() {
        return this.status.get();
    }

    public synchronized boolean isBusy() {
        assertOpen();
        return busyRefCounter.isBusy();
    }

    public boolean isUpgrading() {
        assertOpen();
        return this.runningUpgradeAction.get() != null;
    }

    public void addTicket(ItemTicket<K, V, Ctx> ticket) {
        assertOpen();
        synchronized (this) {
            final boolean add = this.tickets.add(ticket);
            if (!add) {
                throw new IllegalStateException("Ticket already exists");
            }
            createFutures();
        }
        if (ticket.getTargetStatus().ordinal() <= this.getStatus().ordinal()) {
            ticket.consumeCallback();
        }
    }

    public synchronized void removeTicket(ItemTicket<K, V, Ctx> ticket) {
        assertOpen();
        final boolean remove = this.tickets.remove(ticket);
        if (!remove) {
            throw new IllegalStateException("Ticket does not exist");
        }
        createFutures();
    }

    public void submitOp(CompletionStage<Void> op) {
        assertOpen();
//        this.opFuture.set(opFuture.get().thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
//        this.opFuture.getAndUpdate(future -> future.thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
        this.busyRefCounter.incrementRefCount();
        op.whenComplete((unused, throwable) -> this.busyRefCounter.decrementRefCount());
    }

    public void subscribeOp(Completable op) {
        assertOpen();
        this.busyRefCounter.incrementRefCount();
        op.onErrorComplete().subscribe(this.busyRefCounter::decrementRefCount);
    }

    public void submitUpgradeAction(CancellationSignaller signaller) {
        assertOpen();
        final boolean success = this.runningUpgradeAction.compareAndSet(null, signaller);
        Assertions.assertTrue(success, "Only one action can happen at a time");
        signaller.addListener(unused -> this.runningUpgradeAction.set(null));
    }

    public void tryCancelUpgradeAction() {
        assertOpen();
        final CancellationSignaller signaller = this.runningUpgradeAction.get();
        if (signaller != null) {
            signaller.cancel();
        }
    }

    public CompletableFuture<Void> getOpFuture() {
        assertOpen();
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.busyRefCounter.addListener(() -> future.complete(null));
        return future;
    }

    public void submitOpListener(Runnable runnable) {
        assertOpen();
        this.busyRefCounter.addListener(runnable);
    }

    public void setStatus(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        ItemTicket<K, V, Ctx>[] ticketsToFire = null;
        CompletableFuture<Void> futureToFire = null;
        synchronized (this) {
            final ItemStatus<K, V, Ctx> prevStatus = this.getStatus();
            Assertions.assertTrue(status != prevStatus, "duplicate setStatus call");
            this.status.set(status);
            this.statusHistory.add(Pair.of(status, System.currentTimeMillis()));
            final int compare = Integer.compare(status.ordinal(), prevStatus.ordinal());
            if (compare < 0) { // status downgrade
                Assertions.assertTrue(prevStatus.getPrev() == status, "Invalid status downgrade");

                final ItemStatus<K, V, Ctx> targetStatus = this.getTargetStatus();
                for (int i = prevStatus.ordinal(); i < this.futures.length; i ++) {
                    if (i > targetStatus.ordinal()) {
                        this.futures[i].completeExceptionally(UNLOADED_EXCEPTION);
                        this.futures[i] = UNLOADED_FUTURE;
                    } else {
                        this.futures[i] = this.futures[i].isDone() ? new CompletableFuture<>() : this.futures[i];
                    }
                }
            } else if (compare > 0) { // status upgrade
                Assertions.assertTrue(prevStatus.getNext() == status, "Invalid status upgrade");

                final CompletableFuture<Void> future = this.futures[status.ordinal()];

                Assertions.assertTrue(future != UNLOADED_FUTURE);
                Assertions.assertTrue(!future.isDone());
                futureToFire = future;
                ticketsToFire = this.tickets.getTicketsForStatus(status).toArray(ItemTicket[]::new);
            }
        }
        if (ticketsToFire != null) {
            for (ItemTicket<K, V, Ctx> ticket : ticketsToFire) {
                ticket.consumeCallback();
            }
        }
        if (futureToFire != null) {
            futureToFire.complete(null);
        }
    }

    public synchronized void setDependencies(ItemStatus<K, V, Ctx> status, KeyStatusPair<K, V, Ctx>[] dependencies) {
        assertOpen();
        final int ordinal = status.ordinal();
        if (dependencies != null) {
            Assertions.assertTrue(this.requestedDependencies[ordinal] == null, "Duplicate setDependencies call");
            this.requestedDependencies[ordinal] = dependencies;
        } else {
            Assertions.assertTrue(this.requestedDependencies[ordinal] != null, "Duplicate setDependencies call");
            this.requestedDependencies[ordinal] = null;
        }
    }

    public synchronized KeyStatusPair<K, V, Ctx>[] getDependencies(ItemStatus<K, V, Ctx> status) {
        assertOpen();
        return this.requestedDependencies[status.ordinal()];
    }

    public K getKey() {
        return this.key;
    }

    public synchronized CompletableFuture<Void> getFutureForStatus(ItemStatus<K, V, Ctx> status) {
        return this.futures[status.ordinal()].thenApply(Function.identity());
    }
    
    public AtomicReference<V> getItem() {
        return this.item;
    }

    public AtomicReference<UserData> getUserData() {
        assertOpen();
        return this.userData;
    }

    public int getFlags() {
        return this.flags.get();
    }

    public void setFlag(int flag) {
        assertOpen();
        this.flags.getAndUpdate(operand -> operand | flag);
    }

    public void release(SimpleObjectPool<TicketSet<K, V, Ctx>> ticketSetPool) {
        setFlag(FLAG_REMOVED);
        ticketSetPool.release(this.tickets);
    }

    private void assertOpen() {
        Assertions.assertTrue((this.getFlags() & FLAG_REMOVED) == 0);
    }
}
