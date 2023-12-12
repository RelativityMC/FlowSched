package com.ishland.flowsched.scheduler;

import java.util.concurrent.CompletionStage;

/**
 * Represents the status of an item.
 * <p>
 * Implementations must also implement {@link Comparable}
 *
 * @param <Ctx> the context type
 */
public interface ItemStatus<Ctx> {

    ItemStatus<Ctx> getPrev();

    ItemStatus<Ctx> getNext();

    CompletionStage<Void> upgradeToThis(Ctx context);

    CompletionStage<Void> downgradeFromThis(Ctx context);

}
