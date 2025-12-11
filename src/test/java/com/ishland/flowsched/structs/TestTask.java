package com.ishland.flowsched.structs;

import com.ishland.flowsched.executor.LockToken;
import com.ishland.flowsched.executor.Task;

public class TestTask extends Task {
    @Override
    public void run(Runnable releaseLocks) {
        releaseLocks.run();
    }

    @Override
    public void propagateException(Throwable t) {
        t.printStackTrace();
    }

    @Override
    public LockToken[] lockTokens() {
        return new LockToken[0];
    }
}
