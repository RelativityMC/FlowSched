package com.ishland.flowsched.executor;

import com.ishland.flowsched.structs.DynamicPriorityQueue;
import com.ishland.flowsched.structs.SimpleObjectPool;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArraySet;

import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class ExecutorManager {

    private final DynamicPriorityQueue<Task> globalWorkQueue = new DynamicPriorityQueue<>(256);
    private final Object2ReferenceOpenHashMap<LockToken, Set<Task>> lockListeners = new Object2ReferenceOpenHashMap<>();
    private final SimpleObjectPool<Set<Task>> lockListenersPool = new SimpleObjectPool<>(
            pool -> new ReferenceArraySet<>(32),
            Set::clear,
            Set::clear,
            4096
    );
    private final Object schedulingMutex = new Object();
    final Object workerMonitor = new Object();
    private final WorkerThread[] workerThreads;

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
        synchronized (this.schedulingMutex) {
            for (LockToken token : task.lockTokens()) {
                final Set<Task> listeners = this.lockListeners.get(token);
                if (listeners != null) {
                    listeners.add(task);
                    return false;
                }
            }
            for (LockToken token : task.lockTokens()) {
                assert !this.lockListeners.containsKey(token);
                this.lockListeners.put(token, this.lockListenersPool.alloc());
            }
            return true;
        }
    }

    /**
     * Release the locks held by the given task.
     * @param task the task.
     */
    void releaseLocks(Task task) {
        synchronized (this.schedulingMutex) {
            for (LockToken token : task.lockTokens()) {
                final Set<Task> listeners = this.lockListeners.remove(token);
                if (listeners != null) {
                    for (Task listener : listeners) {
                        this.schedule(listener);
                    }
                    this.lockListenersPool.release(listeners);
                } else {
                    throw new IllegalStateException("Lock token " + token + " is not locked");
                }
            }
        }
    }

    /**
     * Polls an executable task from the global work queue.
     * @return the task, or {@code null} if no task is executable.
     */
    Task pollExecutableTask() {
        synchronized (this.schedulingMutex) {
            Task task;
            while ((task = this.globalWorkQueue.dequeue()) != null) {
                if (this.tryLock(task)) {
                    return task;
                }
            }
        }
        return null;
    }

    /**
     * Shuts down the executor manager.
     */
    public void shutdown() {
        for (WorkerThread workerThread : workerThreads) {
            workerThread.shutdown();
        }
    }

    /**
     * Schedules a task.
     * @param task the task.
     */
    public void schedule(Task task) {
        synchronized (this.schedulingMutex) {
            this.globalWorkQueue.enqueue(task, task.priority());
        }
        synchronized (this.workerMonitor) {
            this.workerMonitor.notify();
        }
    }

    /**
     * Schedules a runnable for execution with the given priority.
     *
     * @param runnable the runnable.
     * @param priority the priority.
     */
    public void schedule(Runnable runnable, int priority) {
        this.schedule(new SimpleTask(runnable, priority));
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
    public void notifyPriorityChange(Task task) {
        synchronized (this.schedulingMutex) {
            this.globalWorkQueue.changePriority(task, task.priority());
        }
    }

}
