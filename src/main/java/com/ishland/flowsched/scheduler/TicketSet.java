package com.ishland.flowsched.scheduler;

import com.ishland.flowsched.util.Assertions;

import java.lang.invoke.VarHandle;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TicketSet<K, V, Ctx> {

//    private static final AtomicIntegerFieldUpdater<TicketSet> targetStatusUpdater = AtomicIntegerFieldUpdater.newUpdater(TicketSet.class, "targetStatus");

    private final ItemStatus<K, V, Ctx> initialStatus;
    private final Set<ItemTicket<K, V, Ctx>>[] status2Tickets;
//    private volatile int targetStatus = 0;

    public TicketSet(ItemStatus<K, V, Ctx> initialStatus) {
        this.initialStatus = initialStatus;
//        this.targetStatus = initialStatus.ordinal();
        ItemStatus<K, V, Ctx>[] allStatuses = initialStatus.getAllStatuses();
        this.status2Tickets = new Set[allStatuses.length];
        for (int i = 0; i < allStatuses.length; i++) {
            this.status2Tickets[i] = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }
        VarHandle.fullFence();
    }

    public boolean add(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean added = this.status2Tickets[targetStatus.ordinal()].add(ticket);
        if (!added) return false;

//        if (this.targetStatus < targetStatus.ordinal()) {
//            this.targetStatus = targetStatus.ordinal();
//        }
//        targetStatusUpdater.accumulateAndGet(this, targetStatus.ordinal(), Math::max);
        return true;
    }

    public boolean remove(ItemTicket<K, V, Ctx> ticket) {
        ItemStatus<K, V, Ctx> targetStatus = ticket.getTargetStatus();
        final boolean removed = this.status2Tickets[targetStatus.ordinal()].remove(ticket);
        if (!removed) return false;

//        decreaseStatusAtomically();

        return true;
    }

//    private void decreaseStatusAtomically() {
//        int currentStatus;
//        int newStatus;
//        do {
//            currentStatus = this.targetStatus;
//            newStatus = currentStatus;
//
//            while (newStatus > 0 && this.status2Tickets[newStatus].isEmpty()) {
//                newStatus--;
//            }
//
//            if (newStatus >= currentStatus) {
//                break; // no need to update
//            }
//
//        } while (!targetStatusUpdater.compareAndSet(this, currentStatus, newStatus));
//    }

    public ItemStatus<K, V, Ctx> getTargetStatus() {
        return this.initialStatus.getAllStatuses()[this.computeTargetStatusSlow()];
    }

    public Set<ItemTicket<K, V, Ctx>> getTicketsForStatus(ItemStatus<K, V, Ctx> status) {
        return this.status2Tickets[status.ordinal()];
    }

    void clear() {
        for (Set<ItemTicket<K, V, Ctx>> tickets : status2Tickets) {
            tickets.clear();
        }
//        this.targetStatus = initialStatus.ordinal();
        VarHandle.fullFence();
    }

    void assertEmpty() {
        for (Set<ItemTicket<K, V, Ctx>> tickets : status2Tickets) {
            Assertions.assertTrue(tickets.isEmpty());
        }
    }

    private int computeTargetStatusSlow() {
        for (int i = this.status2Tickets.length - 1; i > 0; i--) {
            if (!this.status2Tickets[i].isEmpty()) {
                return i;
            }
        }
        return 0;
    }

}
