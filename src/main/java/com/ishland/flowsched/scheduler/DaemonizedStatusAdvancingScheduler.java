package com.ishland.flowsched.scheduler;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public abstract class DaemonizedStatusAdvancingScheduler<K, V, Ctx> extends StatusAdvancingScheduler<K, V, Ctx> {

    private final Thread thread;
    private final Object notifyMonitor = new Object();
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    private final Executor executor = taskQueue::add;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public DaemonizedStatusAdvancingScheduler(ThreadFactory threadFactory) {
        this.thread = threadFactory.newThread(this::mainLoop);
        this.thread.start();
    }

    private void mainLoop() {
        main_loop:
        while (true) {
            if (this.shutdown.get()) {
                return;
            }
            if (pollTasks()) {
                continue;
            }

            // attempt to spin-wait before sleeping
            if (!pollTasks()) {
                Thread.interrupted(); // clear interrupt flag
                for (int i = 0; i < 1000; i ++) {
                    if (tick()) continue main_loop;
                    LockSupport.parkNanos("Spin-waiting for tasks", 10_000); // 10us
                }
            }

//            LockSupport.parkNanos("Waiting for tasks", 1_000_000); // 1ms
            synchronized (this.notifyMonitor) {
                if (this.hasPendingUpdates()) continue main_loop;
                try {
                    this.notifyMonitor.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private boolean pollTasks() {
        boolean hasWork = false;
        Runnable runnable;
        while ((runnable = taskQueue.poll()) != null) {
            hasWork = true;
            try {
                runnable.run();
            } catch (Throwable t) {
                t.printStackTrace(); // TODO exception handling
            }
        }
        hasWork |= super.tick();
        return hasWork;
    }

    @Override
    protected final Executor getExecutor() {
        return this.executor;
    }

    @Override
    protected boolean hasPendingUpdates() {
        return !this.taskQueue.isEmpty() || super.hasPendingUpdates();
    }

    @Override
    protected void markDirty(K key) {
        super.markDirty(key);
        synchronized (this.notifyMonitor) {
            this.notifyMonitor.notify();
        }
    }

    public void shutdown() {
        shutdown.set(true);
        LockSupport.unpark(thread);
    }

}
