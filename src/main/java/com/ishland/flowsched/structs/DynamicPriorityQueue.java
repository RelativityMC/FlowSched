package com.ishland.flowsched.structs;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;

/**
 * A priority queue with fixed number of priorities and allows changing priorities of elements.
 * Not thread-safe.
 *
 * @param <E> the type of elements held in this collection
 */
public class DynamicPriorityQueue<E> {

    private final ObjectLinkedOpenHashSet<E>[] priorities;
    private final Object2IntMap<E> priorityMap = new Object2IntOpenHashMap<>();

    private int currentMinPriority = 0;

    public DynamicPriorityQueue(int priorityCount) {
        //noinspection unchecked
        this.priorities = new ObjectLinkedOpenHashSet[priorityCount];
        for (int i = 0; i < priorityCount; i++) {
            this.priorities[i] = new ObjectLinkedOpenHashSet<>();
        }
    }

    public synchronized void enqueue(E element, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");
        if (priorityMap.containsKey(element))
            throw new IllegalArgumentException("Element already in queue");

        if (!priorities[priority].add(element)) throw new AssertionError("Element already in priority " + priority);
        priorityMap.put(element, priority);
        if (priority < currentMinPriority)
            currentMinPriority = priority;
    }

    public synchronized boolean changePriority(E element, int priority) {
        if (priority < 0 || priority >= priorities.length)
            throw new IllegalArgumentException("Priority out of range");
        if (!priorityMap.containsKey(element)) return false; // ignored

        int oldPriority = priorityMap.getInt(element);
        if (oldPriority == priority) return false; // nothing to do

        if (!priorities[oldPriority].remove(element)) throw new AssertionError("Element not found in priority " + oldPriority);
        if (!priorities[priority].addAndMoveToLast(element)) throw new AssertionError("Element already in priority " + priority);
        priorityMap.put(element, priority);

        if (priority < currentMinPriority) currentMinPriority = priority;
        return true;
    }

    public synchronized E dequeue() {
        while (currentMinPriority < priorities.length) {
            ObjectLinkedOpenHashSet<E> priority = this.priorities[currentMinPriority];
            if (priority.isEmpty()) {
                currentMinPriority++;
                continue;
            }
            E element = priority.removeFirst();
            priorityMap.removeInt(element);
            return element;
        }
        return null;
    }

    public synchronized boolean contains(E element) {
        return priorityMap.containsKey(element);
    }

    public synchronized void remove(E element) {
        if (!priorityMap.containsKey(element))
            return; // ignore
        int priority = priorityMap.removeInt(element);
        if (!priorities[priority].remove(element)) throw new AssertionError("Element not found in priority " + priority);
    }

    public int size() {
        return priorityMap.size();
    }

}
