package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.DaemonizedStatusAdvancingScheduler;
import com.ishland.flowsched.scheduler.ItemHolder;
import com.ishland.flowsched.scheduler.ItemStatus;
import com.ishland.flowsched.scheduler.KeyStatusPair;
import com.ishland.flowsched.scheduler.StatusAdvancingScheduler;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class TestSchedulerImpl extends DaemonizedStatusAdvancingScheduler<Long, TestItem, TestContext> {

    public TestSchedulerImpl(ThreadFactory threadFactory) {
        super(threadFactory);
    }

    @Override
    protected ItemStatus<TestContext> getUnloadedStatus() {
        return TestStatus.STATE_0;
    }

    @Override
    protected Collection<KeyStatusPair<Long, TestContext>> getDependencies(ItemHolder<Long, TestItem, TestContext> holder, ItemStatus<TestContext> status) {
        return List.of();
    }

    @Override
    protected TestContext makeContext(ItemHolder<Long, TestItem, TestContext> holder, ItemStatus<TestContext> nextStatus) {
        return new TestContext(holder.getKey());
    }
}
