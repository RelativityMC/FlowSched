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

    ItemStatus<Ctx> getPrev();

    ItemStatus<Ctx> getNext();

    Collection<ItemStatus<Ctx>> getAllStatuses();

    CompletionStage<Void> upgradeToThis(Ctx context);

    CompletionStage<Void> downgradeFromThis(Ctx context);

}
