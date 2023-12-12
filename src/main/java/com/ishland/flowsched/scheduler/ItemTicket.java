package com.ishland.flowsched.scheduler;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ItemTicket<K, Ctx> implements Comparable<ItemTicket<K, Ctx>> {

    private final K source;
    private final ItemStatus<Ctx> targetStatus;
    private final AtomicReference<Runnable> callback = new AtomicReference<>();

    public ItemTicket(K source, ItemStatus<Ctx> targetStatus, Runnable callback) {
        this.source = Objects.requireNonNull(source);
        this.targetStatus = Objects.requireNonNull(targetStatus);
        this.callback.set(callback);
    }

    public K getSource() {
        return this.source;
    }

    public ItemStatus<Ctx> getTargetStatus() {
        return this.targetStatus;
    }

    public void consumeCallback() {
        Runnable callback = this.callback.getAndSet(null);
        if (callback != null) {
            callback.run();
        }
    }

    @Override
    public int compareTo(ItemTicket<K, Ctx> o) {
        final int compare = ((Comparable<ItemStatus<Ctx>>) this.targetStatus).compareTo(o.targetStatus);
        return compare != 0 ? compare : ((Comparable<K>) this.source).compareTo(o.source);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemTicket<?, ?> that = (ItemTicket<?, ?>) o;
        return Objects.equals(source, that.source) && Objects.equals(targetStatus, that.targetStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, targetStatus);
    }
}
