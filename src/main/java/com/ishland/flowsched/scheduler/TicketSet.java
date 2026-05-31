package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

/**
 * Not thread-safe
 */
public class TicketSet<K, V, Ctx> {

    private final ItemStatus<K, V, Ctx> initialStatus;
    private final Set<ItemTicket>[] status2Tickets;
    private final int[] status2TicketsSize;
    private volatile int targetStatus = 0;

    public TicketSet(ItemStatus<K, V, Ctx> initialStatus, ObjectFactory objectFactory) {
        this.initialStatus = initialStatus;
        this.targetStatus = initialStatus.ordinal();
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.status2Tickets = new Set[allStatuses.length];
        this.status2TicketsSize = new int[allStatuses.length];
        VarHandle.fullFence();
    }

    public boolean checkAdd(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        final boolean added = this.getOrCreateTicketsForStatus(targetStatus.ordinal()).add(ticket);
        return added;
    }

    public void addUnchecked(ItemStatus<K, V, Ctx> targetStatus) {
        this.status2TicketsSize[targetStatus.ordinal()] ++;
        if (targetStatus.ordinal() > this.targetStatus) {
            this.targetStatus = targetStatus.ordinal();
        }
    }

    public boolean checkRemove(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        final Set<ItemTicket> tickets = this.status2Tickets[targetStatus.ordinal()];
        return tickets != null && tickets.remove(ticket);
    }

    public void removeUnchecked(ItemStatus<K, V, Ctx> targetStatus) {
        int updated = --this.status2TicketsSize[targetStatus.ordinal()];
        if (updated == 0) {
            this.updateTargetStatus();
        }
    }

    private void updateTargetStatus() {
        this.targetStatus = this.computeTargetStatusSlow();
    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.targetStatus];
    }

    public Set<ItemTicket> getTicketsForStatus(ItemStatus<K, V, Ctx> status) {
        final Set<ItemTicket> tickets = this.status2Tickets[status.ordinal()];
        return tickets != null ? tickets : Collections.emptySet();
    }

    void clear() {
        Arrays.fill(this.status2Tickets, null);
        Arrays.fill(this.status2TicketsSize, 0);
        this.targetStatus = this.initialStatus.ordinal();

        VarHandle.fullFence();
    }

    void assertEmpty() {
        for (Set<ItemTicket> tickets : status2Tickets) {
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

    private Set<ItemTicket> getOrCreateTicketsForStatus(int statusOrdinal) {
        Set<ItemTicket> tickets = this.status2Tickets[statusOrdinal];
        if (tickets == null) {
            tickets = new ObjectOpenHashSet<>(ObjectOpenHashSet.DEFAULT_INITIAL_SIZE, ObjectOpenHashSet.FAST_LOAD_FACTOR);
            this.status2Tickets[statusOrdinal] = tickets;
        }
        return tickets;
    }

}
