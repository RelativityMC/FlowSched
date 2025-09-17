package com.ishland.flowsched.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class WorkerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger("FlowSched Executor Worker Thread");

    private final ExecutorManager executorManager;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public WorkerThread(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    public void run() {
        main_loop:
        while (true) {
            this.executorManager.waitObj.acquireUninterruptibly();

            if (this.shutdown.get()) {
                return;
            }
            while (!this.shutdown.get() && !pollTasks()) {
                Thread.onSpinWait();
            }
        }
    }

    private boolean pollTasks() {
        Task task = this.executorManager.getGlobalWorkQueue().dequeue();
        if (task == null) {
            return false;
        }
        if (!this.executorManager.tryLock(task)) {
            return true; // polled
        }
        try {
            AtomicBoolean released = new AtomicBoolean(false);
            try {
                task.run(() -> {
                    if (released.compareAndSet(false, true)) {
                        executorManager.releaseLocks(task);
                    }
                });
            } catch (Throwable t) {
                try {
                    if (released.compareAndSet(false, true)) {
                        executorManager.releaseLocks(task);
                    }
                } catch (Throwable t1) {
                    t.addSuppressed(t1);
                    LOGGER.error("Exception thrown while releasing locks", t);
                }
                try {
                    task.propagateException(t);
                } catch (Throwable t1) {
                    t.addSuppressed(t1);
                    LOGGER.error("Exception thrown while propagating exception", t);
                }
            }
            return true;
        } catch (Throwable t) {
            LOGGER.error("Exception thrown while executing task", t);
            return true;
        }
    }

    public void shutdown() {
        shutdown.set(true);
    }


}
