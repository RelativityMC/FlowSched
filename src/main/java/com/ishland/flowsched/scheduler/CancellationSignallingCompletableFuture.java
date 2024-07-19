package com.ishland.flowsched.scheduler;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class CancellationSignallingCompletableFuture<T> extends CompletableFuture<T> {

    private final CompletableFuture<T> wrapped;
    private final Runnable onCancel;

    public CancellationSignallingCompletableFuture(CompletableFuture<T> wrapped, Runnable onCancel) {
        this.onCancel = Objects.requireNonNull(onCancel);
        this.wrapped = wrapped;
        wrapped.whenComplete((t, throwable) -> {
            if (throwable != null) {
                this.completeExceptionally(throwable);
            } else {
                this.complete(t);
            }
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (wrapped.isDone() || this.isDone()) return false;
        final boolean cancelled = super.cancel(mayInterruptIfRunning);
        if (cancelled) {
            onCancel.run();
        }
        return cancelled;
    }
}
