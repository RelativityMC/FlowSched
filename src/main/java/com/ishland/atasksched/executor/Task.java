package com.ishland.atasksched.executor;

public interface Task {

    void run();

    void propagateException(Throwable t);

    LockToken[] lockTokens();

    int priority();

}
