package com.ishland.flowsched.scheduler;

public record KeyStatusPair<K, Ctx>(K key, ItemStatus<Ctx> status) {
}
