package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.scheduler.support.TestSchedulerImpl;
import com.ishland.flowsched.scheduler.support.TestStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.concurrent.locks.LockSupport;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class SchedulerTest {

    @Test
    public void testSimple() {
        final TestSchedulerImpl scheduler = new TestSchedulerImpl();
        long startTime = System.nanoTime();
//        scheduler.addTicket(100L, TestStatus.STATE_3, () -> {
//            System.out.println("100 reached STATE_3 after " + (System.nanoTime() - startTime) + "ns");
//            scheduler.removeTicket(100L, TestStatus.STATE_3);
//        });
        final long key = 1000L;
        scheduler.addTicket(key, TestStatus.STATE_7, () -> {
            System.out.println("reached STATE_7 after " + (System.nanoTime() - startTime) + "ns");
            scheduler.removeTicket(key, TestStatus.STATE_7);

            new Thread(() -> {
                LockSupport.parkNanos(100_000_000);
                long start2 = System.nanoTime();
                scheduler.addTicket(key, TestStatus.STATE_8, () -> {
                    System.out.println("reached STATE_8 after " + (System.nanoTime() - startTime) + "ns");
                    scheduler.removeTicket(key, TestStatus.STATE_8);
                });
                scheduler.getHolder(key).getFutureForStatus0(TestStatus.STATE_8).whenComplete((unused, throwable) -> {
                    if (throwable != null) throwable.printStackTrace();
                    System.out.println("reached STATE_8 (future) after " + (System.nanoTime() - startTime) + "ns");
                });
//                scheduler.waitTickSync();
                System.out.println("task 2 initial submission took " + (System.nanoTime() - start2) + "ns");
            }).start();
        });
        scheduler.getHolder(key).getFutureForStatus0(TestStatus.STATE_7).whenComplete((unused, throwable) -> {
            if (throwable != null) throwable.printStackTrace();
            System.out.println("reached STATE_7 (future) after " + (System.nanoTime() - startTime) + "ns");
        });

//        scheduler.waitTickSync();
        System.out.println("task 1 initial submission took " + (System.nanoTime() - startTime) + "ns");

        for (TestStatus value : TestStatus.values()) {
            if (value.ordinal() == 0) continue;
            scheduler.getHolder(key).getFutureForStatus(value).thenRun(() -> {
                System.out.println("reached " + value + " after " + (System.nanoTime() - startTime) + "ns");
            });
        }

        while (scheduler.itemCount() != 0) {
            LockSupport.parkNanos(1_000_000L);
        }

        System.out.println("All unloaded after " + (System.nanoTime() - startTime) + "ns");
//        scheduler.shutdown();
    }

}
