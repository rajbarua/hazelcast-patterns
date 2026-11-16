package com.hz.patterns.scattergather;

import com.hz.patterns.scattergather.ScatterGather.ItemResult;
import com.hz.patterns.scattergather.ScatterGather.LineStatus;
import com.hz.patterns.scattergather.ScatterGather.OrderResult;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.Util.entry;

final class GatherState implements Serializable {
    private final Map<String, LineStatus> lines = new HashMap<>();
    private int expectedCount;

    Map.Entry<String, OrderResult> add(String orderId, ItemResult result) {
        if (lines.isEmpty()) {
            expectedCount = result.expectedCount();
        } else if (expectedCount != result.expectedCount()) {
            throw new IllegalArgumentException("Conflicting item counts for " + orderId);
        }

        lines.putIfAbsent(result.key().itemId(), result.status());
        return isComplete() ? entry(orderId, new OrderResult(lines)) : null;
    }

    boolean isComplete() {
        return expectedCount > 0 && lines.size() == expectedCount;
    }
}
