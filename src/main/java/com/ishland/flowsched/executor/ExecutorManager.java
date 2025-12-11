package com.ishland.flowsched.executor;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class ExecutorManager {

    private final DynamicPriorityTaskQueue<Task> globalWorkQueue;
    private final ConcurrentMap<LockToken, FreeableTaskList> lockListeners = new ConcurrentHashMap<>();
    private final WorkerThread[] workerThreads;
    public final Semaphore waitObj = new Semaphore(0);

    /**
     * Creates a new executor manager.
     *
     * @param workerThreadCount the number of worker threads.
     */
    public ExecutorManager(int workerThreadCount) {
        this(workerThreadCount, thread -> {});
    }

    /**
     * Creates a new executor manager.
     *
     * @param workerThreadCount the number of worker threads.
     * @param threadInitializer the thread initializer.
     */
    public ExecutorManager(int workerThreadCount, Consumer<Thread> threadInitializer) {
        this(workerThreadCount, threadInitializer, 64);
    }

    /**
     * Creates a new executor manager.
     *
     * @param workerThreadCount the number of worker threads.
     * @param threadInitializer the thread initializer.
     * @param priorityCount the number of priorities.
     */
    public ExecutorManager(int workerThreadCount, Consumer<Thread> threadInitializer, int priorityCount) {
        globalWorkQueue = new DynamicPriorityTaskQueue<>(priorityCount);
        workerThreads = new WorkerThread[workerThreadCount];
        for (int i = 0; i < workerThreadCount; i++) {
            final WorkerThread thread = new WorkerThread(this);
            threadInitializer.accept(thread);
            thread.start();
            workerThreads[i] = thread;
        }
    }

    /**
     * Attempt to lock the given tokens.
     * The caller should discard the task if this method returns false, as it reschedules the task.
     *
     * @return {@code true} if the lock is acquired, {@code false} otherwise.
     */
    boolean tryLock(Task task) {
        retry:
        while (true) {
            final FreeableTaskList listenerSet = new FreeableTaskList();
            LockToken[] lockTokens = task.lockTokens();
            for (int i = 0; i < lockTokens.length; i++) {
                LockToken token = lockTokens[i];
                final FreeableTaskList present = this.lockListeners.putIfAbsent(token, listenerSet);
                if (present != null) {
                    for (int j = 0; j < i; j++) {
                        this.lockListeners.remove(lockTokens[j], listenerSet);
                    }
                    callListeners(listenerSet); // synchronizes
                    synchronized (present) {
                        if (present.freed) {
                            continue retry;
                        } else {
                            present.add(task);
                        }
                    }
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Release the locks held by the given task.
     * @param task the task.
     */
    void releaseLocks(Task task) {
        FreeableTaskList expectedListeners = null;
        for (LockToken token : task.lockTokens()) {
            final FreeableTaskList listeners = this.lockListeners.remove(token);
            if (listeners != null) {
                if (expectedListeners == null) {
                    expectedListeners = listeners;
                } else {
                    Assertions.assertTrue(expectedListeners == listeners, "Inconsistent lock listeners");
                }
            } else {
                throw new IllegalStateException("Lock token " + token + " is not locked");
            }
        }
        if (expectedListeners != null) {
            callListeners(expectedListeners); // synchronizes
        }
    }

    private void callListeners(FreeableTaskList listeners) {
        synchronized (listeners) {
            listeners.freed = true;
            if (listeners.isEmpty()) return;
            for (Task listener : listeners) {
                listener.reset();
                this.schedule0(listener, listener.pendingPriority);
            }
        }
    }

    /**
     * Polls an executable task from the global work queue.
     * @return the task, or {@code null} if no task is executable.
     */
    Task pollExecutableTask() {
        Task task;
        while ((task = this.globalWorkQueue.dequeue()) != null) {
            if (this.tryLock(task)) {
                return task;
            }
        }
        return null;
    }

    DynamicPriorityTaskQueue<Task> getGlobalWorkQueue() {
        return this.globalWorkQueue;
    }

    /**
     * Shuts down the executor manager.
     */
    public void shutdown() {
        for (WorkerThread workerThread : workerThreads) {
            workerThread.shutdown();
        }
        this.waitObj.release(workerThreads.length * 128);
    }

    /**
     * Schedules a task.
     * @param task the task.
     */
    public void schedule(Task task, int priority) {
        schedule0(task, priority);
    }

    private void schedule0(Task task, int priority) {
        task.pendingPriority = priority;
        this.globalWorkQueue.enqueue(task, priority);
        this.waitObj.release(1);
    }

    /**
     * Schedules a runnable for execution with the given priority.
     *
     * @param runnable the runnable.
     * @param priority the priority.
     */
    public void schedule(Runnable runnable, int priority) {
        this.schedule(new SimpleTask(runnable), priority);
    }

    /**
     * Creates an executor that schedules runnables with the given priority.
     *
     * @param priority the priority.
     * @return the executor.
     */
    public Executor executor(int priority) {
        return runnable -> this.schedule(runnable, priority);
    }

    /**
     * Notifies the executor manager that the priority of the given task has changed.
     *
     * @param task the task.
     */
    public void changePriority(Task task, int priority) {
        task.pendingPriority = priority;
        this.globalWorkQueue.changePriority(task, priority);
    }

    private static class FreeableTaskList extends ReferenceArrayList<Task> {

        private boolean freed = false;

    }

}
