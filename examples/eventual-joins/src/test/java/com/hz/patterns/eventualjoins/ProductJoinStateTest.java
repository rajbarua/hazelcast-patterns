package com.hz.patterns.eventualjoins;

import com.hz.patterns.eventualjoins.ProductJoinState.OrderArrived;
import com.hz.patterns.eventualjoins.ProductJoinState.ProductArrived;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.hazelcast.jet.Util.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProductJoinStateTest {
    @Test
    void productCompletesEachWaitingOrderOnceAndRemainsAvailable() {
        var state = new ProductJoinState();
        var first = new OrderArrived("book", "first");
        var product = new ProductArrived("book", "Notebook");

        assertNull(state.accept(first));
        assertNull(state.accept(first));
        assertNull(state.accept(new OrderArrived("book", "second")));

        var joined = new JoinResult("Notebook", 0);
        assertEquals(List.of(entry("first", joined), entry("second", joined)), state.accept(product));
        assertNull(state.accept(product));
        assertEquals(List.of(entry("third", joined)), state.accept(new OrderArrived("book", "third")));
    }

    @Test
    void productCanArriveBeforeAnyOrders() {
        var state = new ProductJoinState();

        assertNull(state.accept(new ProductArrived("pen", "Fountain pen")));
        assertEquals(List.of(entry("first", new JoinResult("Fountain pen", 0))),
                state.accept(new OrderArrived("pen", "first")));
    }
}
