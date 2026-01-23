package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Cancellable {

    @SuppressWarnings("unused")
    private Runnable onCancel;

    public void setup(Runnable onCancel) {
        final var result = VH_CANCEL.getAndSet(this, onCancel);
        Assertions.assertTrue(result != COMPLETED, "Cancellation is already completed when setup");
    }

    public boolean complete() {
        while (true) {
            final var witness = (Runnable) VH_CANCEL.getAcquire(this);
            if (witness == CANCELLED || witness == COMPLETED) {
                return false;
            }
            if (VH_CANCEL.weakCompareAndSetRelease(this, witness, COMPLETED)) {
                return true;
            }
        }
    }

    public boolean cancel() {
        while (true) {
            final Runnable witness = (Runnable) VH_CANCEL.get(this);
            if (witness == CANCELLED || witness == COMPLETED) {
                return false;
            }
            if (VH_CANCEL.weakCompareAndSetRelease(this, witness, CANCELLED)) {
                if (witness != null) {
                    VarHandle.acquireFence();
                    witness.run();
                }
                return true;
            }
        }
    }

    public boolean isCancelled() {
        return VH_CANCEL.getAcquire(this) == CANCELLED;
    }

    public boolean isCompleted() {
        return VH_CANCEL.getAcquire(this) == COMPLETED;
    }

    private static final VarHandle VH_CANCEL;
    private static final Runnable CANCELLED = () -> {};
    private static final Runnable COMPLETED = () -> {};

    static {
        try {
            VH_CANCEL = MethodHandles.lookup().findVarHandle(Cancellable.class, "onCancel", Runnable.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
