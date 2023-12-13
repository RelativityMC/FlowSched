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
    protected ItemStatus<TestContext> getUnloadedStatus() {
        return TestStatus.STATE_0;
    }

    @Override
    protected Collection<KeyStatusPair<Long, TestContext>> getDependencies(ItemHolder<Long, TestItem, TestContext> holder, ItemStatus<TestContext> status) {
        final ItemStatus<TestContext> prev = status.getPrev();
        if (prev == null || prev == getUnloadedStatus()) return List.of();
        List<KeyStatusPair<Long, TestContext>> deps = new ArrayList<>();
        for (long i = 0; i < holder.getKey(); i ++) {
            deps.add(new KeyStatusPair<>(i, prev));
        }
//        System.out.println(String.format("Dependencies of %d at %s: %s", holder.getKey(), status, deps));
        return deps;
    }

    @Override
    protected TestContext makeContext(ItemHolder<Long, TestItem, TestContext> holder, ItemStatus<TestContext> nextStatus) {
        return new TestContext(holder.getKey());
    }
}
