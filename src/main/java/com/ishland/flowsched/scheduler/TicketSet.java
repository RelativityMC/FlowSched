package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.lang.invoke.VarHandle;

public class TicketSet<K, V, Ctx> {

    private final ItemStatus<K, V, Ctx> initialStatus;
    private final ObjectOpenHashSet<ItemTicket<K, V, Ctx>>[] status2Tickets;
    private int targetStatus = 0;

    public TicketSet(ItemStatus<K, V, Ctx> initialStatus) {
        this.initialStatus = initialStatus;
        this.targetStatus = initialStatus.ordinal();
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.status2Tickets = new ObjectOpenHashSet[allStatuses.length];
        for (int i = 0; i < allStatuses.length; i++) {
            this.status2Tickets[i] = new ObjectOpenHashSet<>() {
                @Override
                protected void rehash(int newN) {
                    if (n < newN) {
                        super.rehash(newN);
                    }
                }
            };
        }
        VarHandle.fullFence();
    }

    public boolean add(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean added = this.status2Tickets[targetStatus.ordinal()].add(ticket);
        if (!added) return false;

        if (this.targetStatus < targetStatus.ordinal()) {
            this.targetStatus = targetStatus.ordinal();
        }
        return true;
    }

    public boolean remove(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean removed = this.status2Tickets[targetStatus.ordinal()].remove(ticket);
        if (!removed) return false;

        while (this.status2Tickets[this.targetStatus].isEmpty()) {
            if ((-- this.targetStatus) <= 0) {
                break;
            }
        }

        return true;
    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.targetStatus];
    }

    public ObjectSet<ItemTicket<K, V, Ctx>> getTicketsForStatus(ItemStatus<K, V, Ctx> status) {
        return this.status2Tickets[status.ordinal()];
    }

    void clear() {
        for (ObjectOpenHashSet<ItemTicket<K, V, Ctx>> tickets : status2Tickets) {
            tickets.clear();
        }
        this.targetStatus = initialStatus.ordinal();
        VarHandle.fullFence();
    }

    void assertEmpty() {
        for (ObjectOpenHashSet<ItemTicket<K, V, Ctx>> tickets : status2Tickets) {
            Assertions.assertTrue(tickets.isEmpty());
        }
    }

}
