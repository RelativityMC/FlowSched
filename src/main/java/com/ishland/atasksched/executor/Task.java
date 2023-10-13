package com.ishland.atasksched.executor;

public interface Task {

    void run();

    LockToken[] lockTokens();

    int priority();

}
