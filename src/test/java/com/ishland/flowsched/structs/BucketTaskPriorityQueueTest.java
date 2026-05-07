package com.ishland.flowsched.structs;

import com.ishland.flowsched.executor.BucketTaskPriorityQueue;
import com.ishland.flowsched.executor.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BucketTaskPriorityQueueTest {

    private BucketTaskPriorityQueue queue;
    private final TestTask a = new TestTask(), b = new TestTask(), c = new TestTask(), d = new TestTask(), e = new TestTask(), f = new TestTask(), g = new TestTask(), h = new TestTask();

    @BeforeEach
    void setUp() {
        queue = new BucketTaskPriorityQueue(256);
        queue.requeue(a, 5);
        queue.requeue(b, 10);
        queue.requeue(c, 20);
        queue.requeue(d, 30);
        queue.requeue(e, 40);
        queue.requeue(f, 50);
        queue.requeue(g, 60);
        queue.requeue(h, 254);
    }

    @Test
    void testDequeue() {
        assertQueue(a, b, c, d, e, f, g, h);
        assertNull(queue.dequeue());
    }

    @Test
    void testrequeue() {
        TestTask i = new TestTask();
        queue.requeue(i, 0);
        assertQueue(i, a, b, c, d, e, f, g, h);
    }

    @Test
    void testChangePriority1() {
        queue.requeue(a, 255);
        queue.requeue(b, 255);
        queue.requeue(c, 255);
        queue.requeue(d, 255);
        queue.requeue(e, 255);
        queue.requeue(f, 255);
        queue.requeue(g, 255);
        queue.requeue(h, 255);
        assertQueue(a, b, c, d, e, f, g, h);
    }

    @Test
    void testChangePriority2() {
        queue.requeue(h, 0);
        queue.requeue(g, 10);
        queue.requeue(f, 20);
        queue.requeue(e, 30);
        queue.requeue(d, 40);
        queue.requeue(c, 50);
        queue.requeue(b, 60);
        queue.requeue(a, 254);
        assertQueue(h, g, f, e, d, c, b, a);
    }

    @Test
    void testAssertions() {
        TestTask i = new TestTask();
        assertThrows(IllegalArgumentException.class, () -> queue.requeue(i, -1)); // attempt to requeue with invalid priority
        assertThrows(IllegalArgumentException.class, () -> queue.requeue(i, 256)); // attempt to requeue with invalid priority
    }

    private void assertQueue(TestTask... expected) {
        for (int i = 0; i < expected.length; i++) {
            retry:
            while (true) {
                Task task = queue.dequeue();
                if (task == Task.TOMBSTONE) continue retry;
                assertEquals(expected[i], task, "Element #" + i + " mismatched");
                break retry;
            }
        }
        if (queue.size() != 0) {
            fail("Queue size mismatched");
        }
    }

}