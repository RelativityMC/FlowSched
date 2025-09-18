package com.ishland.flowsched.scheduler.support;

import com.ishland.flowsched.scheduler.Cancellable;
import com.ishland.flowsched.scheduler.ItemHolder;
import com.ishland.flowsched.scheduler.ItemStatus;
import com.ishland.flowsched.scheduler.KeyStatusPair;
import com.ishland.flowsched.util.Assertions;
import io.reactivex.rxjava3.core.Completable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

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
    public Completable upgradeToThis(TestContext context, Cancellable cancellable) {
//        System.out.println(String.format("Upgrading %d to %s", context.key(), this));
        if (TestSchedulerImpl.GLOBAL_RNG.nextBoolean()) {
            cancellable.cancel();
            return Completable.error(new CancellationException());
        }
        return Completable.complete();
    }

    @Override
    public Completable preDowngradeFromThis(TestContext context, Cancellable cancellable) {
        if ((context.rng() & 1) == 0) {
            cancellable.cancel();
            return Completable.error(new CancellationException());
        }
        return Completable.complete();
    }


    @Override
    public Completable downgradeFromThis(TestContext context, Cancellable cancellable) {
        if ((context.rng() & 1) == 0) {
            Assertions.assertTrue(false, "erroneous call to downgradeFromThis");
        }
//        System.out.println(String.format("Downgrading %d from %s", context.key(), this));
        return Completable.complete();
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
