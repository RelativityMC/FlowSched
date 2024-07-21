package com.ishland.flowsched.scheduler;

import java.util.Objects;

public class ItemTicket<K, V, Ctx> {

    private final TicketType type;
    private final Object source;
    private final ItemStatus<K, V, Ctx> targetStatus;
    private Runnable callback = null;

    public ItemTicket(TicketType type, Object source, ItemStatus<K, V, Ctx> targetStatus, Runnable callback) {
        this.type = type;
        this.source = Objects.requireNonNull(source);
        this.targetStatus = Objects.requireNonNull(targetStatus);
        this.callback = callback;
    }

    public Object getSource() {
        return this.source;
    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.targetStatus;
    }

    public TicketType getType() {
        return this.type;
    }

    public void consumeCallback() {
        if (this.callback == null) return;
        Runnable callback;
        synchronized (this) {
            callback = this.callback;
            this.callback = null;
        }
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
