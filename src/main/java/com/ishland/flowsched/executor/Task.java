package com.ishland.flowsched.executor;

public abstract class Task {

    public static final int P_UNINITIALIZED = -2, P_REMOVED = -1, QID_NOT_YET_QUEUED = -1;

    int queueId = QID_NOT_YET_QUEUED, priority = P_UNINITIALIZED, pendingPriority = P_UNINITIALIZED;

    public abstract void run(Runnable releaseLocks);

    public abstract void propagateException(Throwable t);

    public abstract LockToken[] lockTokens();

    void reset() {
        queueId = QID_NOT_YET_QUEUED;
        priority = P_UNINITIALIZED;
    }
}

