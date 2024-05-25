package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.DaemonizedStatusAdvancingScheduler;
import com.ishland.flowsched.scheduler.ItemHolder;
import com.ishland.flowsched.scheduler.ItemStatus;
import com.ishland.flowsched.scheduler.KeyStatusPair;

import java.util.concurrent.ThreadFactory;

public class TestSchedulerImpl extends DaemonizedStatusAdvancingScheduler<Long, TestItem, TestContext, Void> {

    public TestSchedulerImpl(ThreadFactory threadFactory) {
        super(threadFactory);
    }

    @Override
    protected ItemStatus<Long, TestItem, TestContext> getUnloadedStatus() {
        return TestStatus.STATE_0;
    }

    @Override
    protected TestContext makeContext(ItemHolder<Long, TestItem, TestContext, Void> holder, ItemStatus<Long, TestItem, TestContext> nextStatus, KeyStatusPair<Long, TestItem, TestContext>[] dependencies, boolean isUpgrade) {
        return new TestContext(holder.getKey());
    }
}
