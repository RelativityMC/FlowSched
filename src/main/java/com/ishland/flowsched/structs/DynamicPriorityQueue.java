package com.ishland.flowsched.structs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A priority queue with fixed number of priorities and allows changing priorities of elements.
 *
 * @param <E> the type of elements held in this collection
 */
public class DynamicPriorityQueue<E> {

    private final ConcurrentLinkedQueue<E>[] priorities;
    private final ConcurrentHashMap<E, Integer> priorityMap = new ConcurrentHashMap<>();

    private final AtomicInteger currentMinPriority = new AtomicInteger(0);

    public DynamicPriorityQueue(int priorityCount) {
        //noinspection unchecked
        this.priorities = new ConcurrentLinkedQueue[priorityCount];
        for (int i = 0; i < priorityCount; i++) {
            this.priorities[i] = new ConcurrentLinkedQueue<>();
        }
    }

    public void enqueue(E element, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");
        if (this.priorityMap.putIfAbsent(element, priority) != null)
            throw new IllegalArgumentException("Element already in queue");

        this.priorities[priority].add(element);
        this.currentMinPriority.getAndUpdate(operand -> Math.min(operand, priority));
    }

    // behavior is undefined when changing priority for one item concurrently
    public boolean changePriority(E element, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");

        int currentPriority = this.priorityMap.getOrDefault(element, -1);
        if (currentPriority == -1 || currentPriority == priority) {
            return false; // a clear failure
        }
        final boolean removedFromQueue = this.priorities[currentPriority].remove(element);
        if (!removedFromQueue) {
            return false; // the element is dequeued while we are changing priority
        }
        final Integer put = this.priorityMap.put(element, priority);
        final boolean changeSuccess = put != null && put == currentPriority;
        if (!changeSuccess) {
            return false; // something else may have called remove()
        }
        this.priorities[priority].add(element);
        this.currentMinPriority.getAndUpdate(operand -> Math.min(operand, priority));
        return true;
    }

    public E dequeue() {
        int current = this.currentMinPriority.get();
        while (current < this.priorities.length) {
            E element = priorities[current].poll();
            if (element != null) {
                this.priorityMap.remove(element);
                return element;
            } else {
                int finalCurrent = current;
                current = this.currentMinPriority.updateAndGet(operand -> operand < this.priorities.length ? Math.min(operand + (priorities[operand].isEmpty() ? 1 : 0), finalCurrent + 1) : operand);
            }
        }
        return null;
    }

    public boolean contains(E element) {
        return priorityMap.containsKey(element);
    }

    public void remove(E element) {
        final Integer remove = this.priorityMap.remove(element);
        if (remove == null) return;
        this.priorities[remove].remove(element); // best-effort
    }

    public int size() {
        return priorityMap.size();
    }

}
