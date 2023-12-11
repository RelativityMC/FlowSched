package com.ishland.flowsched.scheduler;

import java.util.concurrent.CompletionStage;

public interface ItemStatus<Ctx> extends Comparable<ItemStatus<Ctx>> {

    ItemStatus<Ctx> getPrev();

    ItemStatus<Ctx> getNext();

    CompletionStage<Void> upgradeToThis(Ctx context);

    CompletionStage<Void> downgradeFromThis(Ctx context);

}
