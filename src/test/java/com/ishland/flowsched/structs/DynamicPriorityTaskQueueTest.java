package com.ishland.flowsched.structs;

import com.ishland.flowsched.executor.DynamicPriorityTaskQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DynamicPriorityTaskQueueTest {

    private DynamicPriorityTaskQueue<TestTask> queue;
    private final TestTask a = new TestTask(), b = new TestTask(), c = new TestTask(), d = new TestTask(), e = new TestTask(), f = new TestTask(), g = new TestTask(), h = new TestTask();

    @BeforeEach
    void setUp() {
        queue = new DynamicPriorityTaskQueue<>(256);
        queue.enqueue(a, 5);
        queue.enqueue(b, 10);
        queue.enqueue(c, 20);
        queue.enqueue(d, 30);
        queue.enqueue(e, 40);
        queue.enqueue(f, 50);
        queue.enqueue(g, 60);
        queue.enqueue(h, 254);
    }

    @Test
    void testDequeue() {
        assertQueue(a, b, c, d, e, f, g, h);
        assertNull(queue.dequeue());
    }

    @Test
    void testEnqueue() {
        TestTask i = new TestTask();
        queue.enqueue(i, 0);
        assertQueue(i, a, b, c, d, e, f, g, h);
    }

    @Test
    void testChangePriority1() {
        queue.changePriority(a, 255);
        queue.changePriority(b, 255);
        queue.changePriority(c, 255);
        queue.changePriority(d, 255);
        queue.changePriority(e, 255);
        queue.changePriority(f, 255);
        queue.changePriority(g, 255);
        queue.changePriority(h, 255);
        assertQueue(a, b, c, d, e, f, g, h);
    }

    @Test
    void testChangePriority2() {
        queue.changePriority(h, 0);
        queue.changePriority(g, 10);
        queue.changePriority(f, 20);
        queue.changePriority(e, 30);
        queue.changePriority(d, 40);
        queue.changePriority(c, 50);
        queue.changePriority(b, 60);
        queue.changePriority(a, 254);
        assertQueue(h, g, f, e, d, c, b, a);
    }

    @Test
    void testContains() {
        TestTask i = new TestTask();
        assertTrue(queue.contains(a));
        assertTrue(queue.contains(b));
        assertTrue(queue.contains(c));
        assertTrue(queue.contains(d));
        assertTrue(queue.contains(e));
        assertTrue(queue.contains(f));
        assertTrue(queue.contains(g));
        assertTrue(queue.contains(h));
        assertFalse(queue.contains(i));
    }

    @Test
    void testRemove() {
        queue.remove(a);
        queue.remove(b);
        queue.remove(c);
        queue.remove(d);
        queue.remove(e);
        queue.remove(f);
        queue.remove(g);
        queue.remove(h);
        assertQueue();
    }

    @Test
    void testSize() {
        assertEquals(8, queue.size());
        queue.remove(a);
        assertEquals(7, queue.size());
        queue.remove(b);
        assertEquals(6, queue.size());
        queue.remove(c);
        assertEquals(5, queue.size());
        queue.remove(d);
        assertEquals(4, queue.size());
        queue.remove(e);
        assertEquals(3, queue.size());
        queue.remove(f);
        assertEquals(2, queue.size());
        queue.remove(g);
        assertEquals(1, queue.size());
        queue.remove(h);
        assertEquals(0, queue.size());
    }

    @Test
    void testAssertions() {
        TestTask i = new TestTask();
        assertThrows(IllegalArgumentException.class, () -> queue.enqueue(i, -1)); // attempt to enqueue with invalid priority
        assertThrows(IllegalArgumentException.class, () -> queue.enqueue(i, 256)); // attempt to enqueue with invalid priority
        assertThrows(IllegalArgumentException.class, () -> queue.enqueue(a, 0)); // attempt to enqueue with existing element
        assertThrows(IllegalArgumentException.class, () -> queue.changePriority(a, -1)); // attempt to change priority with invalid priority
        assertThrows(IllegalArgumentException.class, () -> queue.changePriority(a, 256)); // attempt to change priority with invalid priority
        assertEquals(DynamicPriorityTaskQueue.R_UNINITIALIZED, queue.changePriority(i, 0)); // attempt to change priority with non-existing element
        assertEquals(5, queue.changePriority(a, 5)); // attempt to change priority with same priority
    }

    private void assertQueue(TestTask... expected) {
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], queue.dequeue(), "Element #" + i + " mismatched");
        }
        if (queue.size() != 0) {
            fail("Queue size mismatched");
        }
    }

}