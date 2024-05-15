package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

public class TicketSet<K, Ctx> {

    private final ItemStatus<Ctx> initialStatus;
    private final ObjectOpenHashSet<ItemTicket<K, Ctx>>[] status2Tickets;
    private final AtomicInteger targetStatus = new AtomicInteger();

    public TicketSet(ItemStatus<Ctx> initialStatus) {
        this.initialStatus = initialStatus;
        this.targetStatus.set(initialStatus.ordinal());
        ItemStatus<Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.status2Tickets = new ObjectOpenHashSet[allStatuses.length];
        for (int i = 0; i < allStatuses.length; i++) {
            this.status2Tickets[i] = new ObjectOpenHashSet<>();
        }
        VarHandle.fullFence();
    }

    public boolean add(ItemTicket<K, Ctx> ticket) {
        ItemStatus<Ctx> targetStatus = ticket.getTargetStatus();
        final boolean added = this.status2Tickets[targetStatus.ordinal()].add(ticket);
        if (!added) return false;

        if (this.targetStatus.get() < targetStatus.ordinal()) {
            this.targetStatus.set(targetStatus.ordinal());
        }
        return true;
    }

    public boolean remove(ItemTicket<K, Ctx> ticket) {
        ItemStatus<Ctx> targetStatus = ticket.getTargetStatus();
        final boolean removed = this.status2Tickets[targetStatus.ordinal()].remove(ticket);
        if (!removed) return false;

        while (this.status2Tickets[this.targetStatus.get()].isEmpty()) {
            if (this.targetStatus.decrementAndGet() <= 0) {
                break;
            }
        }
        return true;
    }

    public ItemStatus<Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.targetStatus.get()];
    }

    public ObjectSet<ItemTicket<K, Ctx>> getTicketsForStatus(ItemStatus<Ctx> status) {
        return this.status2Tickets[status.ordinal()];
    }

}
