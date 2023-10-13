package com.ishland.atasksched.executor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class WorkerThread extends Thread {

    private final SchedManager schedManager;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public WorkerThread(SchedManager schedManager) {
        this.schedManager = schedManager;
    }

    @Override
    public void run() {
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
                    if (pollTasks()) continue main_loop;
                    LockSupport.parkNanos("Spin-waiting for tasks", 10_000); // 10us
                }
            }

//            LockSupport.parkNanos("Waiting for tasks", 1_000_000); // 1ms
            synchronized (this.schedManager.workerMonitor) {
                try {
                    this.schedManager.workerMonitor.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private boolean pollTasks() {
        final Task task = schedManager.pollExecutableTask();
        try {
            if (task != null) {
                task.run();
                return true;
            }
            return false;
        } catch (Throwable t) {
            t.printStackTrace();
            return true;
        } finally {
            if (task != null) {
                schedManager.releaseLocks(task);
            }
        }
    }

    public void shutdown() {
        shutdown.set(true);
        LockSupport.unpark(this);
    }


}
