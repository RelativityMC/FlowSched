package com.ishland.flowsched.structs;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class OneTaskAtATimeExecutor implements Executor {

    private final AtomicBoolean currentlyRunning = new AtomicBoolean(false);
    private final Queue<Runnable> queue;
    private final Executor backingExecutor;
    private final Runnable task = this::run0;

    public OneTaskAtATimeExecutor(Queue<Runnable> queue, Executor backingExecutor) {
        this.backingExecutor = backingExecutor;
        this.queue = queue;
    }

    private boolean canRun() {
        return !this.queue.isEmpty();
    }

    private void run0() {
        try {
            Runnable command;
            while ((command = this.queue.poll()) != null) {
                try {
                    command.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        } finally {
            this.currentlyRunning.set(false);
            this.trySchedule();
        }
    }

    private void trySchedule() {
        if (!this.queue.isEmpty() && this.needsWakeup()) {
            this.backingExecutor.execute(this.task);
        }
    }

    private boolean needsWakeup() {
        return this.currentlyRunning.compareAndSet(false, true);
    }

    @Override
    public void execute(Runnable command) {
        this.queue.add(command);
        this.trySchedule();
    }
}
