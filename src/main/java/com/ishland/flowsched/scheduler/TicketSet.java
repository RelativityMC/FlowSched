package com.ishland.flowsched.scheduler;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

public class TicketSet<K, V, Ctx> {

    private final ItemStatus<K, V, Ctx> initialStatus;
    private final ObjectOpenHashSet<ItemTicket<K, V, Ctx>>[] status2Tickets;
    private final AtomicInteger targetStatus = new AtomicInteger();

    public TicketSet(ItemStatus<K, V, Ctx> initialStatus) {
        this.initialStatus = initialStatus;
        this.targetStatus.set(initialStatus.ordinal());
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.status2Tickets = new ObjectOpenHashSet[allStatuses.length];
        for (int i = 0; i < allStatuses.length; i++) {
            this.status2Tickets[i] = new ObjectOpenHashSet<>();
        }
        VarHandle.fullFence();
    }

    public boolean add(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean added = this.status2Tickets[targetStatus.ordinal()].add(ticket);
        if (!added) return false;

        if (this.targetStatus.get() < targetStatus.ordinal()) {
            this.targetStatus.set(targetStatus.ordinal());
        }
        return true;
    }

    public boolean remove(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean removed = this.status2Tickets[targetStatus.ordinal()].remove(ticket);
        if (!removed) return false;

        while (this.status2Tickets[this.targetStatus.get()].isEmpty()) {
            if (this.targetStatus.decrementAndGet() <= 0) {
                break;
            }
        }
        return true;
    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.targetStatus.get()];
    }

    public ObjectSet<ItemTicket<K, V, Ctx>> getTicketsForStatus(ItemStatus<K, V, Ctx> status) {
        return this.status2Tickets[status.ordinal()];
    }

}
