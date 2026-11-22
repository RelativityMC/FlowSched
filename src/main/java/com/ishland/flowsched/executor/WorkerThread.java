package com.ishland.flowsched.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger("FlowSched Executor Worker Thread");

    private final ExecutorManager executorManager;
    private final Runnable threadPrologue;
    private volatile boolean shutdown = false;

    public WorkerThread(ExecutorManager executorManager, Runnable threadPrologue) {
        this.executorManager = executorManager;
        this.threadPrologue = threadPrologue;
    }

    @Override
    public void run() {
        if (this.threadPrologue != null) {
            try {
                this.threadPrologue.run();
            } catch (Throwable t) {
                LOGGER.error("Error in threadPrologue", t);
            }
        }

        main_loop:
        while (true) {
            this.executorManager.waitObj.acquireUninterruptibly();

            if (this.shutdown) {
                return;
            }
            while (!this.shutdown && !pollTasks()) {
                Thread.onSpinWait();
            }
        }
    }

    private boolean pollTasks() {
        Task task = this.executorManager.doDequeueFIFO();
        if (task == null) {
            return false;
        }

        // always return true below because task have been polled
        if (task == Task.TOMBSTONE) {
            return true;
        }

        final boolean p0 = task.polledAtPriority == 0;
        if (p0) {
            this.executorManager.p0Workers.incrementAndGet();
        }
        try {
            this.executorManager.executeTask0(task);
        } finally {
            if (p0) {
                this.executorManager.p0Workers.decrementAndGet();
            }
        }
        return true;
    }

    public void shutdown() {
        shutdown = true;
    }


}
