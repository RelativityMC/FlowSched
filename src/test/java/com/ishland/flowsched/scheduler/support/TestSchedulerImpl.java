package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.ItemHolder;
import com.ishland.flowsched.scheduler.ItemStatus;
import com.ishland.flowsched.scheduler.KeyStatusPair;
import com.ishland.flowsched.scheduler.StatusAdvancingScheduler;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class TestSchedulerImpl extends StatusAdvancingScheduler<Long, TestItem, TestContext, Void> {

    static final Random GLOBAL_RNG = new Random();

    private static final ThreadFactory factory = Executors.defaultThreadFactory();
    private static final ExecutorService backgroundExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2, r -> {
        final Thread thread = factory.newThread(r);
        thread.setDaemon(true);
        return thread;
    });
    private static final Scheduler backgroundScheduler = Schedulers.from(backgroundExecutor);

    public TestSchedulerImpl() {
    }

    @Override
    protected Executor getBackgroundExecutor() {
        return backgroundExecutor;
    }

    @Override
    protected Scheduler getSchedulerBackedByBackgroundExecutor() {
        return backgroundScheduler;
    }

    @Override
    protected ItemStatus<Long, TestItem, TestContext> getUnloadedStatus() {
        return TestStatus.STATE_0;
    }

    @Override
    protected TestContext makeContext(ItemHolder<Long, TestItem, TestContext, Void> holder, ItemStatus<Long, TestItem, TestContext> nextStatus, KeyStatusPair<Long, TestItem, TestContext>[] dependencies, boolean isUpgrade) {
        return new TestContext(holder.getKey(), GLOBAL_RNG.nextInt());
    }
}
