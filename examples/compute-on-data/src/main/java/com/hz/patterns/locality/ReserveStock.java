package com.hz.patterns.locality;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.EntryProcessor;
import java.util.Map;

public final class ReserveStock implements EntryProcessor<StockKey, Integer, Reservation>,
        HazelcastInstanceAware {
    private final int quantity;
    private transient HazelcastInstance member;

    public ReserveStock(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        this.quantity = quantity;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance member) {
        this.member = member;
    }

    @Override
    public Reservation process(Map.Entry<StockKey, Integer> entry) {
        Integer before = entry.getValue();
        boolean accepted = before != null && before >= quantity;
        Integer after = before;
        if (accepted) {
            after = before - quantity;
            entry.setValue(after);
        }
        return new Reservation(accepted, before, after, member.getCluster().getLocalMember().getUuid());
    }
}
