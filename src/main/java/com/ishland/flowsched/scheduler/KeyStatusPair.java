package com.ishland.flowsched.scheduler;

public record KeyStatusPair<K, V, Ctx>(K key, ItemStatus<K, V, Ctx> status) {
}
