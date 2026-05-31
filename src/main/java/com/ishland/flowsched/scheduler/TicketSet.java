package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class TicketSet<K, V, Ctx> {

    private final ItemStatus<K, V, Ctx> initialStatus;
    private final ObjectFactory objectFactory;
    private final Set<ItemTicket<K, V, Ctx>>[] status2Tickets;
    private final int[] status2TicketsSize;
    private volatile int targetStatus = 0;

    public TicketSet(ItemStatus<K, V, Ctx> initialStatus, ObjectFactory objectFactory) {
        this.initialStatus = initialStatus;
        this.objectFactory = objectFactory;
        this.targetStatus = initialStatus.ordinal();
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.status2Tickets = new Set[allStatuses.length];
        this.status2TicketsSize = new int[allStatuses.length];
        VarHandle.fullFence();
    }

    public boolean checkAdd(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean added = this.getOrCreateTicketsForStatus(targetStatus.ordinal()).add(ticket);
        return added;
    }

    /**
     * Not thread-safe
     */
    public void addUnchecked(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        this.status2TicketsSize[targetStatus.ordinal()] ++;
        this.updateTargetStatus();
    }

    public boolean checkRemove(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final Set<ItemTicket<K, V, Ctx>> tickets = this.status2Tickets[targetStatus.ordinal()];
        return tickets != null && tickets.remove(ticket);
    }

    /**
     * Not thread-safe
     */
    public void removeUnchecked(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        this.status2TicketsSize[targetStatus.ordinal()] --;
        this.updateTargetStatus();
    }

    private void updateTargetStatus() {
        this.targetStatus = this.computeTargetStatusSlow();
    }

    /**
     * Not thread-safe
     */
    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.targetStatus];
    }

    public Set<ItemTicket<K, V, Ctx>> getTicketsForStatus(ItemStatus<K, V, Ctx> status) {
        final Set<ItemTicket<K, V, Ctx>> tickets = this.status2Tickets[status.ordinal()];
        return tickets != null ? tickets : Collections.emptySet();
    }

    void clear() {
        Arrays.fill(this.status2Tickets, null);
        Arrays.fill(this.status2TicketsSize, 0);
        this.targetStatus = this.initialStatus.ordinal();

        VarHandle.fullFence();
    }

    void assertEmpty() {
        for (Set<ItemTicket<K, V, Ctx>> tickets : status2Tickets) {
            Assertions.assertTrue(tickets == null || tickets.isEmpty());
        }
    }

    private int computeTargetStatusSlow() {
        for (int i = this.status2TicketsSize.length - 1; i > 0; i--) {
            if (this.status2TicketsSize[i] > 0) {
                return i;
            }
        }
        return this.initialStatus.ordinal();
    }

    private Set<ItemTicket<K, V, Ctx>> getOrCreateTicketsForStatus(int statusOrdinal) {
        Set<ItemTicket<K, V, Ctx>> tickets = this.status2Tickets[statusOrdinal];
        if (tickets == null) {
            tickets = this.objectFactory.createConcurrentSet();
            this.status2Tickets[statusOrdinal] = tickets;
        }
        return tickets;
    }

}
