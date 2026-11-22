package com.ishland.flowsched.executor;

import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A priority queue with fixed number of priorities and allows changing priorities of elements.
 */
public class BucketTaskPriorityQueue {

    private final AtomicIntegerArray taskCount;
    private final ConcurrentLinkedQueue<Task>[] priorities;

    public BucketTaskPriorityQueue(int priorityCount) {
        this.taskCount = new AtomicIntegerArray(priorityCount);
        //noinspection unchecked
        this.priorities = new ConcurrentLinkedQueue[priorityCount];
        for (int i = 0; i < priorityCount; i++) {
            this.priorities[i] = new ConcurrentLinkedQueue<>();
        }
        VarHandle.fullFence();
    }

    private void enqueue0(Task element, int priority) {
        this.priorities[priority].add(element);
        this.taskCount.incrementAndGet(priority);
    }

    public Task dequeue() {
        for (int i = 0; i < this.priorities.length; i ++) {
            Task task = this.dequeue(i);
            if (task != null) return task;
        }
        return null;
    }

    Task dequeue(int priority) {
        if (this.taskCount.get(priority) == 0) return null;

        ConcurrentLinkedQueue<Task> queue = this.priorities[priority];
        Task element = queue.poll();
        if (element == null) return null;

        if (!element.pollAtPriority(priority)) {
            element = Task.TOMBSTONE;
        }

        this.taskCount.decrementAndGet(priority);
        return element;
    }

    public boolean requeue(Task task, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");

        task.pendingPriority = priority;
        if (task.changePriority(priority)) {
            this.enqueue0(task, priority);
            return true;
        } else {
            return false;
        }
    }

    public int size() {
        int estimate = 0;
        for (int i = 0; i < this.priorities.length; i++) {
            estimate += taskCount.get(i);
        }
        return estimate;
    }

}


