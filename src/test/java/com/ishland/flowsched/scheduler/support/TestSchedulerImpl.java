package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.DaemonizedStatusAdvancingScheduler;
import com.ishland.flowsched.scheduler.ItemHolder;
import com.ishland.flowsched.scheduler.ItemStatus;
import com.ishland.flowsched.scheduler.KeyStatusPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class TestSchedulerImpl extends DaemonizedStatusAdvancingScheduler<Long, TestItem, TestContext> {

    public TestSchedulerImpl(ThreadFactory threadFactory) {
        super(threadFactory);
    }

    @Override
    protected ItemStatus<Long, TestItem, TestContext> getUnloadedStatus() {
        return TestStatus.STATE_0;
    }

    @Override
    protected TestContext makeContext(ItemHolder<Long, TestItem, TestContext> holder, ItemStatus<Long, TestItem, TestContext> nextStatus) {
        return new TestContext(holder.getKey());
    }
}
