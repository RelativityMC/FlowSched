package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.lang.invoke.VarHandle;
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
        for (int i = 0; i < allStatuses.length; i++) {
            this.status2Tickets[i] = new ObjectOpenHashSet<>(ObjectOpenHashSet.DEFAULT_INITIAL_SIZE, ObjectOpenHashSet.FAST_LOAD_FACTOR);
        }
        this.status2TicketsSize = new int[allStatuses.length];
        VarHandle.fullFence();
    }

    public boolean checkAdd(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        final boolean added = this.status2Tickets[targetStatus.ordinal()].add(ticket);
        return added;
    }

    public void addUnchecked(ItemStatus<K, V, Ctx> targetStatus) {
        this.status2TicketsSize[targetStatus.ordinal()] ++;
        if (targetStatus.ordinal() > this.targetStatus) {
            this.targetStatus = targetStatus.ordinal();
        }
    }

    public boolean checkRemove(ItemStatus<K, V, Ctx> targetStatus, ItemTicket ticket) {
        final boolean removed = this.status2Tickets[targetStatus.ordinal()].remove(ticket);
        return removed;
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
        return this.status2Tickets[status.ordinal()];
    }

    void clear() {
        for (Set<ItemTicket> tickets : status2Tickets) {
            tickets.clear();
        }

        VarHandle.fullFence();
    }

    void assertEmpty() {
        for (Set<ItemTicket> tickets : status2Tickets) {
            Assertions.assertTrue(tickets.isEmpty());
        }
    }

    private int computeTargetStatusSlow() {
        for (int i = this.status2Tickets.length - 1; i > 0; i--) {
            if (this.status2TicketsSize[i] > 0) {
                return i;
            }
        }
        return 0;
    }

}
