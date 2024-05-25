package com.ishland.flowsched.scheduler;

import java.util.concurrent.CompletionStage;

/**
 * Represents the status of an item.
 * <p>
 * Implementations must also implement {@link Comparable}, and higher statuses must be greater than lower statuses.
 *
 * @param <Ctx> the context type
 */
public interface ItemStatus<K, V, Ctx> {

    default ItemStatus<K, V, Ctx> getPrev() {
        return this.ordinal() > 0 ? getAllStatuses()[this.ordinal() - 1] : null;
    }

    default ItemStatus<K, V, Ctx> getNext() {
        final ItemStatus<K, V, Ctx>[] allStatuses = getAllStatuses();
        return this.ordinal() < allStatuses.length - 1 ? allStatuses[this.ordinal() + 1] : null;
    }

    ItemStatus<K, V, Ctx>[] getAllStatuses();

    int ordinal();

    CompletionStage<Void> upgradeToThis(Ctx context);

    CompletionStage<Void> downgradeFromThis(Ctx context);

    /**
     * Get the dependencies of the given item at the given status.
     * <p>
     * The returned collection must not contain the given item itself.
     *
     * @param holder the item holder
     * @return the dependencies
     */
    KeyStatusPair<K, V, Ctx>[] getDependencies(ItemHolder<K, V, Ctx, ?> holder);

}
