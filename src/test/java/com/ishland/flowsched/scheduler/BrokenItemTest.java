package com.ishland.flowsched.scheduler;

import io.reactivex.rxjava3.core.Completable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Requests on a holder that got marked broken must not wait forever.
 */
public class BrokenItemTest {

    private static final long TIMEOUT_SECONDS = 5;
    private static final long KEY = 1L;

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(2, r -> {
        final Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    record Ctx(FailureInjectingScheduler scheduler) {
    }

    enum Status implements ItemStatus<Long, Object, Ctx> {
        NEW,
        A,
        B,
        C,
        ;

        private static final ItemStatus<Long, Object, Ctx>[] ALL_STATUSES = values();

        @Override
        public ItemStatus<Long, Object, Ctx>[] getAllStatuses() {
            return ALL_STATUSES;
        }

        @Override
        public Completable upgradeToThis(Ctx context, Cancellable cancellable) {
            return context.scheduler().failUpgradeTo == this ? Completable.error(context.scheduler().upgradeFailure) : Completable.complete();
        }

        @Override
        public Completable postUpgradeToThis(Ctx context) {
            return context.scheduler().failPostUpgradeTo == this ? Completable.error(context.scheduler().postUpgradeFailure) : Completable.complete();
        }

        @Override
        public Completable preDowngradeFromThis(Ctx context, Cancellable cancellable) {
            return Completable.complete();
        }

        @Override
        public Completable downgradeFromThis(Ctx context, Cancellable cancellable) {
            return Completable.complete();
        }

        @Override
        public KeyStatusPair<Long, Object, Ctx>[] getDependencies(ItemHolder<Long, Object, Ctx, ?> holder) {
            return new KeyStatusPair[0];
        }
    }

    static class FailureInjectingScheduler extends StatusAdvancingScheduler<Long, Object, Ctx, Void> {

        final RuntimeException upgradeFailure = new RuntimeException("injected upgrade failure");
        final RuntimeException postUpgradeFailure = new RuntimeException("injected post-upgrade failure");
        volatile Status failUpgradeTo = null;
        volatile Status failPostUpgradeTo = null;

        @Override
        protected Executor getBackgroundExecutor() {
            return EXECUTOR;
        }

        @Override
        protected ItemStatus<Long, Object, Ctx> getUnloadedStatus() {
            return Status.NEW;
        }

        @Override
        protected Ctx makeContext(ItemHolder<Long, Object, Ctx, Void> holder, ItemStatus<Long, Object, Ctx> nextStatus, KeyStatusPair<Long, Object, Ctx>[] dependencies, boolean isUpgrade) {
            return new Ctx(this);
        }

        @Override
        protected ExceptionHandlingAction handleTransactionException(ItemHolder<Long, Object, Ctx, Void> holder, ItemStatus<Long, Object, Ctx> nextStatus, boolean isUpgrade, Throwable throwable) {
            return ExceptionHandlingAction.MARK_BROKEN;
        }
    }

    private static Throwable awaitFailure(CompletableFuture<Void> future) throws Exception {
        final ExecutionException exception = Assertions.assertThrows(ExecutionException.class, () -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        return exception.getCause();
    }

    private static void awaitUnloaded(StatusAdvancingScheduler<?, ?, ?, ?> scheduler) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (scheduler.itemCount() != 0) {
            Assertions.assertTrue(System.nanoTime() < deadline, "holder was not removed");
            LockSupport.parkNanos(1_000_000L);
        }
    }

    private static boolean isBroken(ItemHolder<?, ?, ?, ?> holder) {
        return (holder.getFlags() & ItemHolder.FLAG_BROKEN) != 0;
    }

    @Test
    public void testSuccessfulUpgradeCompletesFutures() throws Exception {
        final FailureInjectingScheduler scheduler = new FailureInjectingScheduler();
        final ItemHolder<Long, Object, Ctx, Void> holder = scheduler.addTicket(KEY, Status.C, null);

        holder.getFutureForStatus0(Status.C).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(holder.getFutureForStatus0(Status.A).isDone());
        Assertions.assertTrue(holder.getFutureForStatus0(Status.B).isDone());
        Assertions.assertFalse(isBroken(holder));

        scheduler.removeTicket(KEY, Status.C);
        awaitUnloaded(scheduler);
    }

    @Test
    public void testUpgradeFailureFailsRequestedFutures() throws Exception {
        final FailureInjectingScheduler scheduler = new FailureInjectingScheduler();
        scheduler.failUpgradeTo = Status.B;
        final ItemHolder<Long, Object, Ctx, Void> holder = scheduler.addTicket(KEY, Status.C, null);
        final CompletableFuture<Void> futureA = holder.getFutureForStatus0(Status.A);
        final CompletableFuture<Void> futureB = holder.getFutureForStatus0(Status.B);
        final CompletableFuture<Void> futureC = holder.getFutureForStatus0(Status.C);

        // the ticket stays, so the futures must not wait for it to be withdrawn
        Assertions.assertSame(scheduler.upgradeFailure, awaitFailure(futureB));
        Assertions.assertSame(scheduler.upgradeFailure, awaitFailure(futureC));
        futureA.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(isBroken(holder));
        Assertions.assertSame(Status.A, holder.getStatus());
        Assertions.assertSame(Status.C, holder.getTargetStatus());

        scheduler.removeTicket(KEY, Status.C);
        awaitUnloaded(scheduler);
    }

    @Test
    public void testTicketAddedAfterBreakFails() throws Exception {
        final FailureInjectingScheduler scheduler = new FailureInjectingScheduler();
        scheduler.failUpgradeTo = Status.B;
        final ItemHolder<Long, Object, Ctx, Void> holder = scheduler.addTicket(KEY, Status.B, null);
        Assertions.assertSame(scheduler.upgradeFailure, awaitFailure(holder.getFutureForStatus0(Status.B)));

        final Object lateSource = new Object();
        Assertions.assertSame(holder, scheduler.addTicket(KEY, lateSource, Status.C, null));
        Assertions.assertSame(scheduler.upgradeFailure, awaitFailure(holder.getFutureForStatus0(Status.C)));

        scheduler.removeTicket(KEY, ItemTicket.TicketType.EXTERNAL, lateSource, Status.C);
        scheduler.removeTicket(KEY, Status.B);
        awaitUnloaded(scheduler);
    }

    @Test
    public void testFreshHolderCanRetryAfterUnload() throws Exception {
        final FailureInjectingScheduler scheduler = new FailureInjectingScheduler();
        scheduler.failUpgradeTo = Status.B;
        final ItemHolder<Long, Object, Ctx, Void> broken = scheduler.addTicket(KEY, Status.B, null);
        Assertions.assertSame(scheduler.upgradeFailure, awaitFailure(broken.getFutureForStatus0(Status.B)));
        scheduler.removeTicket(KEY, Status.B);
        awaitUnloaded(scheduler);

        scheduler.failUpgradeTo = null;
        final ItemHolder<Long, Object, Ctx, Void> holder = scheduler.addTicket(KEY, Status.B, null);
        Assertions.assertNotSame(broken, holder);
        holder.getFutureForStatus0(Status.B).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertFalse(isBroken(holder));

        scheduler.removeTicket(KEY, Status.B);
        awaitUnloaded(scheduler);
    }

    @Test
    public void testPostUpgradeFailureFailsRequestedFutures() throws Exception {
        final FailureInjectingScheduler scheduler = new FailureInjectingScheduler();
        scheduler.failPostUpgradeTo = Status.B;
        final ItemHolder<Long, Object, Ctx, Void> holder = scheduler.addTicket(KEY, Status.C, null);

        Assertions.assertSame(scheduler.postUpgradeFailure, awaitFailure(holder.getFutureForStatus0(Status.C)));
        Assertions.assertTrue(isBroken(holder));
        // B was reached, then forcibly downgraded; the re-created future for B must not wait forever either
        Assertions.assertSame(Status.A, holder.getStatus());
        Assertions.assertSame(scheduler.postUpgradeFailure, awaitFailure(holder.getFutureForStatus0(Status.B)));

        scheduler.removeTicket(KEY, Status.C);
        awaitUnloaded(scheduler);
    }

}
