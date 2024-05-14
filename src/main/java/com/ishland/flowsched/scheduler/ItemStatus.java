package com.ishland.flowsched.scheduler;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Represents the status of an item.
 * <p>
 * Implementations must also implement {@link Comparable}, and higher statuses must be greater than lower statuses.
 *
 * @param <Ctx> the context type
 */
public interface ItemStatus<Ctx> {

    default ItemStatus<Ctx> getPrev() {
        return this.ordinal() > 0 ? getAllStatuses()[this.ordinal() - 1] : null;
    }

    default ItemStatus<Ctx> getNext() {
        final ItemStatus<Ctx>[] allStatuses = getAllStatuses();
        return this.ordinal() < allStatuses.length - 1 ? allStatuses[this.ordinal() + 1] : null;
    }

    ItemStatus<Ctx>[] getAllStatuses();

    int ordinal();

    CompletionStage<Void> upgradeToThis(Ctx context);

    CompletionStage<Void> downgradeFromThis(Ctx context);

}
