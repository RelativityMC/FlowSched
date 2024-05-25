package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.ItemHolder;
import com.ishland.flowsched.scheduler.ItemStatus;
import com.ishland.flowsched.scheduler.KeyStatusPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public enum TestStatus implements ItemStatus<Long, TestItem, TestContext>, Comparable<TestStatus> {
    STATE_0,
    STATE_1,
    STATE_2,
    STATE_3,
    STATE_4,
    STATE_5,
    STATE_6,
    STATE_7,
    STATE_8,
    ;

    public static final ItemStatus<Long, TestItem, TestContext>[] All_STATUSES = List.of(values()).toArray(ItemStatus[]::new);

    @Override
    public ItemStatus<Long, TestItem, TestContext>[] getAllStatuses() {
        return All_STATUSES;
    }

    @Override
    public CompletionStage<Void> upgradeToThis(TestContext context) {
//        System.out.println(String.format("Upgrading %d to %s", context.key(), this));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> downgradeFromThis(TestContext context) {
//        System.out.println(String.format("Downgrading %d from %s", context.key(), this));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public KeyStatusPair<Long, TestItem, TestContext>[] getDependencies(ItemHolder<Long, TestItem, TestContext, ?> holder) {
        final ItemStatus<Long, TestItem, TestContext> prev = this.getPrev();
        if (prev == null || prev == STATE_0) return new KeyStatusPair[0];
        List<KeyStatusPair<Long, TestItem, TestContext>> deps = new ArrayList<>();
        for (long i = 0; i < holder.getKey(); i ++) {
            deps.add(new KeyStatusPair<>(i, prev));
        }
//        System.out.println(String.format("Dependencies of %d at %s: %s", holder.getKey(), status, deps));
        return deps.toArray(KeyStatusPair[]::new);
    }
}
