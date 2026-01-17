package com.ishland.flowsched.scheduler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class ItemTicket {

    private static final AtomicReferenceFieldUpdater<ItemTicket, Runnable> CALLBACK_UPDATER = AtomicReferenceFieldUpdater.newUpdater(ItemTicket.class, Runnable.class, "callback");

    private final int hashCode;
    private final TicketType type;
    private final Object source;
    private volatile Runnable callback = null;
//    private int hash = 0;

    public ItemTicket(TicketType type, Object source, Runnable callback) {
        this.type = Objects.requireNonNull(type);
        this.source = Objects.requireNonNull(source);
        this.callback = callback;
        this.hashCode = this.hashCode0();
    }

    public Object getSource() {
        return this.source;
    }

    public TicketType getType() {
        return this.type;
    }

    public void consumeCallback() {
        Runnable callback = CALLBACK_UPDATER.getAndSet(this, null);
        if (callback != null) {
            try {
                callback.run();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemTicket that = (ItemTicket) o;
        return type == that.type && Objects.equals(source, that.source);
    }

    private int hashCode0() {
        // inlined version of Objects.hash(type, source, targetStatus)
        int result = 1;

        result = 31 * result + type.hashCode();
        result = 31 * result + source.hashCode();
        return result;
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    public static class TicketType {
        public static TicketType DEPENDENCY = new TicketType("flowsched:dependency");
        public static TicketType EXTERNAL = new TicketType("flowsched:external");

        private final String description;

        public TicketType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return this.description;
        }

        // use default equals() and hashCode()

    }
}
