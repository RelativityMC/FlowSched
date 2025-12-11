package com.ishland.flowsched.executor.test;

import com.ishland.flowsched.executor.ExecutorManager;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class TestThroughput {

    private static final Semaphore semaphore = new Semaphore(1 << 7);

    public static volatile double accumulation = 0;
    public static volatile long[] latency = new long[1 << 20];
    public static AtomicInteger counter = new AtomicInteger();

    public static void main(String[] args) {
        final ExecutorManager manager = new ExecutorManager(4);
        long last = System.nanoTime();
        int schedules = 0;
        final ExecutorService pool = Executors.newFixedThreadPool(4);
        while (true) {
            if (schedules >= 1 << 20) {
                final long now = System.nanoTime();
                System.out.println(String.format("Throughput: %.2f rps, latency: %.2fns, acc: %e", schedules * 1e9 / (now - last), Arrays.stream(latency).average().getAsDouble(), accumulation));
                last = now;
                schedules = 0;
            }
            semaphore.acquireUninterruptibly();
            schedules ++;
            //manager.schedule(new Measurement(), (int) (Math.random() * 64));
            pool.execute(new Measurement());
        }
    }

    static class Measurement implements Runnable {

        private final long start = System.nanoTime();

        @Override
        public void run() {
            final long end = System.nanoTime();
            latency[counter.getAndIncrement() & (latency.length - 1)] = end - start;
            semaphore.release();
        }
    }

}