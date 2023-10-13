package com.ishland.atasksched.executor;

import com.ishland.atasksched.structs.DynamicPriorityQueue;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SchedManager {

    private final DynamicPriorityQueue<Task> globalWorkQueue = new DynamicPriorityQueue<>(256);
    private final Object2ReferenceOpenHashMap<LockToken, Queue<Task>> lockListeners = new Object2ReferenceOpenHashMap<>();
    private final Object schedulingMutex = new Object();
    final Object workerMonitor = new Object();
    private final WorkerThread[] workerThreads;

    public SchedManager(int workerThreadCount) {
        workerThreads = new WorkerThread[workerThreadCount];
        for (int i = 0; i < workerThreadCount; i++) {
            final WorkerThread thread = new WorkerThread(this);
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
    public boolean tryLock(Task task) {
        synchronized (this.schedulingMutex) {
            for (LockToken token : task.lockTokens()) {
                final Queue<Task> listeners = this.lockListeners.get(token);
                if (listeners != null) {
                    listeners.add(task);
                    return false;
                }
            }
            for (LockToken token : task.lockTokens()) {
                assert !this.lockListeners.containsKey(token);
                this.lockListeners.put(token, new ArrayDeque<>());
            }
            return true;
        }
    }

    public void releaseLocks(Task task) {
        synchronized (this.schedulingMutex) {
            for (LockToken token : task.lockTokens()) {
                final Queue<Task> listeners = this.lockListeners.remove(token);
                if (listeners != null) {
                    for (Task listener : listeners) {
                        this.schedule(listener);
                    }
                }
            }
        }
    }

    public Task pollExecutableTask() {
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

    public void shutdown() {
        for (WorkerThread workerThread : workerThreads) {
            workerThread.shutdown();
        }
    }

    public void schedule(Task task) {
        synchronized (this.schedulingMutex) {
            this.globalWorkQueue.enqueue(task, task.priority());
        }
        synchronized (this.workerMonitor) {
            this.workerMonitor.notify();
        }
    }

    public void notifyPriorityChange(Task task) {
        synchronized (this.schedulingMutex) {
            this.globalWorkQueue.changePriority(task, task.priority());
        }
    }

}
