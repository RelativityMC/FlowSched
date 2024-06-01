package com.ishland.flowsched.scheduler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ItemTicket<K, V, Ctx> {

    private final TicketType type;
    private final Object source;
    private final ItemStatus<K, V, Ctx> targetStatus;
    private final AtomicReference<Runnable> callback = new AtomicReference<>();

    public ItemTicket(TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        this.type = type;
        this.source = Objects.requireNonNull(source);
        this.targetStatus = Objects.requireNonNull(targetStatus);
        this.callback.set(callback);
    }

    public Object getSource() {
        return this.source;
    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.targetStatus;
    }

    public void consumeCallback() {
        Runnable callback = this.callback.getAndSet(null);
        if (callback != null) {
            callback.run();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemTicket<?, ?, ?> that = (ItemTicket<?, ?, ?>) o;
        return type == that.type && Objects.equals(source, that.source) && Objects.equals(targetStatus, that.targetStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, source, targetStatus);
    }

    public enum TicketType {
        EXTERNAL,
        DEPENDENCY, // source: KeyStatusPair: key, targetStatus
    }
}
