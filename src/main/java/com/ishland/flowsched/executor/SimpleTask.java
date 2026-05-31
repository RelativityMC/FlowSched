package com.ishland.flowsched.executor;

import java.util.Objects;

public class SimpleTask implements Task {
    private static final LockToken[] EMPTY_LOCK_TOKENS = new LockToken[0];

    private final Runnable wrapped;
    private final int priority;

    public SimpleTask(Runnable wrapped, int priority) {
        this.wrapped = Objects.requireNonNull(wrapped);
        this.priority = priority;
    }

    @Override
    public void run(Runnable releaseLocks) {
        try {
            wrapped.run();
        } finally {
            releaseLocks.run();
        }
    }

    @Override
    public void propagateException(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public LockToken[] lockTokens() {
        return EMPTY_LOCK_TOKENS;
    }

    @Override
    public int priority() {
        return this.priority;
    }
}
