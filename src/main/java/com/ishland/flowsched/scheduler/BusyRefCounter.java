package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceList;

import java.util.Objects;

public class BusyRefCounter {

    private final ReferenceList<Runnable> onComplete = new ReferenceArrayList<>();
    private int counter = 0;

    public synchronized boolean isBusy() {
        return counter != 0;
    }

    public void addListener(Runnable runnable) {
        Objects.requireNonNull(runnable);
        synchronized (this) {
            if (!isBusy()) {
                runnable.run();
            } else {
                onComplete.add(runnable);
            }
        }
    }

    public synchronized void incrementRefCount() {
        counter ++;
    }

    public void decrementRefCount() {
        Runnable[] onCompleteArray = null;
        synchronized (this) {
            Assertions.assertTrue(counter > 0);
            if (--counter == 0) {
                onCompleteArray = onComplete.toArray(Runnable[]::new);
                onComplete.clear();
            }
        }
        if (onCompleteArray != null) {
            for (Runnable runnable : onCompleteArray) {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
    }

}
