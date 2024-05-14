package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.ItemStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public enum TestStatus implements ItemStatus<TestContext>, Comparable<TestStatus> {
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

    public static final ItemStatus<TestContext>[] All_STATUSES = List.of(values()).toArray(ItemStatus[]::new);

    @Override
    public ItemStatus<TestContext>[] getAllStatuses() {
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
}
