package com.ishland.flowsched.executor;

public class UrgentWorkerThread extends Thread {

    private final ExecutorManager executorManager;
    private volatile boolean shutdown = false;

    UrgentWorkerThread(ExecutorManager executorManager) {
        this.executorManager = executorManager;
    }

    @Override
    public void run() {
        while (!this.shutdown) {
            this.executorManager.urgentWakeup.acquireUninterruptibly();
            if (this.shutdown) {
                return;
            }

            this.executorManager.urgentWakeup.drainPermits();
            while (!this.shutdown) {
                // immediately sleep when anything else is executing urgent tasks, they'll make progress
                // sleeping makes the OS will more likely schedule us when future tasks come
                if (this.executorManager.p0Workers.get() != 0) {
                    break;
                }

                if (!this.executorManager.waitObj.tryAcquire()) {
                    break;
                }

                Task task = this.executorManager.doDequeueAtPriority(0);
                if (task == null) {
                    // we failed to poll at p0, restore lost permit
                    this.executorManager.waitObj.release(1);
                    break;
                }

                this.executorManager.executeTask0(task);
            }
        }
    }

    public void shutdown() {
        shutdown = true;
    }

}
