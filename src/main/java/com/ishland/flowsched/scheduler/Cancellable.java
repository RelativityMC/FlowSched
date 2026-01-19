package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class Cancellable {

    @SuppressWarnings("unused")
    private Runnable onCancel;

    public boolean setup(Runnable onCancel) {
        final var result = VH_CANCEL.compareAndExchangeAcquire(this, null, onCancel);
        Assertions.assertTrue(result != COMPLETED, "Cancellation is already completed when setup");
        return result == null;
    }

    public boolean complete() {
        final var witness = (Runnable) VH_CANCEL.compareAndExchangeRelease(this, null, COMPLETED);
        if (witness == CANCELLED || witness == COMPLETED) {
            return false;
        }
        if (witness == null) {
            return true;
        }
        // Set it to completed if we can; if we failed, it can only be cancel() or another complete()
        return witness == VH_CANCEL.compareAndExchangeRelease(this, witness, COMPLETED);
    }

    public boolean cancel() {
        final Runnable handle = (Runnable) VH_CANCEL.compareAndExchangeRelease(this, null, CANCELLED);
        if (handle == null) {
            return true;
        }
        if (handle != CANCELLED && handle != COMPLETED) {
            handle.run();
            // Set it to cancelled if we can; if we failed, it can only be complete() or another cancel()
            return handle == VH_CANCEL.compareAndExchangeRelease(this, handle, CANCELLED);
        }
        return false;
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
