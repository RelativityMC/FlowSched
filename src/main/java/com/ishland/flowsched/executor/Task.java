package com.ishland.flowsched.executor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public abstract class Task {

    public static final int P_REMOVED = -2, P_UNINITIALIZED = -1;

    int priority = P_UNINITIALIZED, pendingPriority = P_UNINITIALIZED;

    public abstract void run(Runnable releaseLocks);

    public abstract void propagateException(Throwable t);

    public abstract LockToken[] lockTokens();

    void reset() {
        priority = P_UNINITIALIZED;
    }

    boolean pollAtPriority(int priority) {
        while (true) {
            int curPriority = (int) VH_PRIORITY.getAcquire(this);
            if (priority != curPriority) return false;
            if (priority == (int) VH_PRIORITY.compareAndExchangeAcquire(this, priority, P_REMOVED)) return true;
        }
    }

    boolean changePriority(int newPriority) {
        while (true) {
            int curPriority = (int) VH_PRIORITY.getAcquire(this);
            if (curPriority < P_UNINITIALIZED || curPriority == newPriority) return false;
            if (curPriority == (int) VH_PRIORITY.compareAndExchangeRelease(this, curPriority, newPriority)) return true;
        }
    }

    static final VarHandle VH_PRIORITY;

    static {
        try {
            VH_PRIORITY = MethodHandles.lookup().findVarHandle(Task.class, "priority", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static final Task TOMBSTONE = new Task() {
        @Override
        public void run(Runnable releaseLocks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void propagateException(Throwable t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public LockToken[] lockTokens() {
            throw new UnsupportedOperationException();
        }

        @Override
        void reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean pollAtPriority(int priority) {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean changePriority(int newPriority) {
            throw new UnsupportedOperationException();
        }
    };

}

