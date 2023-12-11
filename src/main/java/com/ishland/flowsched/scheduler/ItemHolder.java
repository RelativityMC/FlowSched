package com.ishland.flowsched.scheduler;

import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

public class ItemHolder<K, V, Ctx> {

    private final K key;
    private final AtomicReference<V> item = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> opFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final ObjectSortedSet<ItemTicket<K, Ctx>> tickets = new ObjectAVLTreeSet<>();
    private final AtomicReference<ItemStatus<Ctx>> status = new AtomicReference<>();

    ItemHolder(ItemStatus<Ctx> initialStatus, K key) {
        this.status.set(Objects.requireNonNull(initialStatus));
        this.key = Objects.requireNonNull(key);
    }

    /**
     * Get the target status of this item.
     *
     * @return the target status of this item, or null if no ticket is present
     */
    public ItemStatus<Ctx> getTargetStatus() {
        return !this.tickets.isEmpty() ? this.tickets.first().getTargetStatus() : null;
    }

    public ItemStatus<Ctx> getStatus() {
        return this.status.get();
    }

    public boolean isBusy() {
        return !this.opFuture.get().isDone();
    }

    public void addTicket(ItemTicket<K, Ctx> ticket) {
        final boolean add = this.tickets.add(ticket);
        if (!add) {
            throw new IllegalStateException("Ticket already exists");
        }
        if (ticket.getTargetStatus().compareTo(this.getStatus()) <= 0) {
            ticket.consumeCallback();
        }
    }

    public void removeTicket(ItemTicket<K, Ctx> ticket) {
        final boolean remove = this.tickets.remove(ticket);
        if (!remove) {
            throw new IllegalStateException("Ticket does not exist");
        }
    }

    public void submitOp(CompletionStage<Void> op) {
        this.opFuture.set(opFuture.get().thenCombine(op, (a, b) -> null).handle((o, throwable) -> null));
    }

    public void setStatus(ItemStatus<Ctx> status) {
        this.status.set(status);
        for (ItemTicket<K, Ctx> ticket : this.tickets) {
            if (status.compareTo(ticket.getTargetStatus()) <= 0) {
                ticket.consumeCallback();
            } else {
                break;
            }
        }
    }

}
