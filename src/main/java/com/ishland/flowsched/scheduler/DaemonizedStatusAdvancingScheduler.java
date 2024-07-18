package com.ishland.flowsched.scheduler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public abstract class DaemonizedStatusAdvancingScheduler<K, V, Ctx, UserData> extends StatusAdvancingScheduler<K, V, Ctx, UserData> {

    private final Thread thread;
    private final Object notifyMonitor = new Object();
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    private final Executor executor = e -> {
        taskQueue.add(e);
        wakeUp();
    };
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public DaemonizedStatusAdvancingScheduler(ThreadFactory threadFactory) {
        this.thread = threadFactory.newThread(this::mainLoop);
        this.thread.start();
    }

    private void mainLoop() {
        main_loop:
        while (true) {
            if (pollTasks()) {
                continue;
            }
            if (this.shutdown.get()) {
                return;
            }

            // attempt to spin-wait before sleeping
            if (!pollTasks()) {
                Thread.interrupted(); // clear interrupt flag
                for (int i = 0; i < 500; i ++) {
                    if (pollTasks()) continue main_loop;
                    LockSupport.parkNanos("Spin-waiting for tasks", 100_000); // 100us
                }
            }

//            LockSupport.parkNanos("Waiting for tasks", 1_000_000); // 1ms
            synchronized (this.notifyMonitor) {
                if (this.hasPendingUpdates() || this.shutdown.get()) continue main_loop;
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

    public void waitTickSync() {
        if (Thread.currentThread() == this.thread) {
            throw new IllegalStateException("Cannot wait sync on scheduler thread");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.getExecutor().execute(() -> future.complete(null));
        future.join();
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
        wakeUp();
    }

    private void wakeUp() {
        synchronized (this.notifyMonitor) {
            this.notifyMonitor.notify();
        }
    }

    public void shutdown() {
        shutdown.set(true);
        wakeUp();
    }

}