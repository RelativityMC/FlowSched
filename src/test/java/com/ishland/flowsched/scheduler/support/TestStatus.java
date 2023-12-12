package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.ItemStatus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public enum TestStatus implements ItemStatus<TestContext>, Comparable<TestStatus> {
    STATE_0,
    STATE_1,
    STATE_2,
    STATE_3,
    STATE_4,
    ;

    @Override
    public ItemStatus<TestContext> getPrev() {
        return this.ordinal() > 0 ? values()[this.ordinal() - 1] : null;
    }

    @Override
    public ItemStatus<TestContext> getNext() {
        return this.ordinal() < values().length - 1 ? values()[this.ordinal() + 1] : null;
    }

    @Override
    public CompletionStage<Void> upgradeToThis(TestContext context) {
        System.out.println(String.format("Upgrading %d to %s", context.key(), this));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> downgradeFromThis(TestContext context) {
        System.out.println(String.format("Downgrading %d from %s", context.key(), this));
        return CompletableFuture.completedFuture(null);
    }
}
