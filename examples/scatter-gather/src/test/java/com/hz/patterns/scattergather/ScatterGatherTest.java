package com.hz.patterns.scattergather;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hz.patterns.scattergather.ScatterGather.ItemKey;
import com.hz.patterns.scattergather.ScatterGather.ItemResult;
import com.hz.patterns.scattergather.ScatterGather.ItemTask;
import com.hz.patterns.scattergather.ScatterGather.LineItem;
import com.hz.patterns.scattergather.ScatterGather.Order;
import com.hz.patterns.scattergather.ScatterGather.OrderResult;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.hz.patterns.scattergather.ScatterGather.ITEM_WORK;
import static com.hz.patterns.scattergather.ScatterGather.LineStatus.ACCEPTED;
import static com.hz.patterns.scattergather.ScatterGather.LineStatus.REJECTED;
import static com.hz.patterns.scattergather.ScatterGather.ORDERS;
import static com.hz.patterns.scattergather.ScatterGather.awaitValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class ScatterGatherTest {
    @Test
    void streamsAndValidatesChildTasksForEachOrder() throws InterruptedException {
        HazelcastInstance member = ScatterGather.startMember();
        try {
            IMap<String, Order> orders = member.getMap(ORDERS);
            IMap<ItemKey, ItemTask> work = member.getMap(ITEM_WORK);
            var bookLine = new LineItem("line-1", "book", 2);
            var secondBookLine = new LineItem("line-2", "book", 0);
            var firstLines = List.of(bookLine, secondBookLine);
            orders.put("order-1", new Order(firstLines));
            member.getJet().newJob(ScatterGather.gatherPipeline());
            member.getJet().newJob(ScatterGather.scatterPipeline());

            OrderResult rejected = new OrderResult(Map.of("line-1", ACCEPTED, "line-2", REJECTED));
            assertEquals(REJECTED, awaitValue(orders, "order-1", new Order(firstLines, rejected)).result().status());

            // The already running scatter job must accept a later order with the same line ID.
            var secondLines = List.of(bookLine);
            orders.put("order-2", new Order(secondLines));
            OrderResult accepted = new OrderResult(Map.of("line-1", ACCEPTED));
            assertEquals(ACCEPTED, awaitValue(orders, "order-2", new Order(secondLines, accepted)).result().status());

            assertEquals(Map.of(
                    new ItemKey("order-1", "line-1"), new ItemTask(bookLine, 2),
                    new ItemKey("order-1", "line-2"), new ItemTask(secondBookLine, 2),
                    new ItemKey("order-2", "line-1"), new ItemTask(bookLine, 1)), work);
            assertEquals(Map.of("order-1", new Order(firstLines, rejected),
                    "order-2", new Order(secondLines, accepted)), orders);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void completedOrderUpdateDoesNotDispatchMoreWork() throws InterruptedException {
        HazelcastInstance member = ScatterGather.startMember();
        try {
            IMap<String, Order> orders = member.getMap(ORDERS);
            IMap<ItemKey, ItemTask> work = member.getMap(ITEM_WORK);
            var originalLine = new LineItem("line-1", "book", 2);
            var firstOrder = new Order(List.of(originalLine));
            orders.put("order-1", firstOrder);
            member.getJet().newJob(ScatterGather.scatterPipeline());
            var firstTask = new ItemKey("order-1", "line-1");
            awaitValue(work, firstTask, new ItemTask(originalLine, 1));

            work.remove(firstTask);
            orders.put("order-1", new Order(firstOrder.items(), new OrderResult(Map.of("line-1", ACCEPTED))));

            // A later write in the same journal partition marks progress past the completed update.
            var partitions = member.getPartitionService();
            int partition = partitions.getPartition("order-1").getPartitionId();
            String nextOrderId = IntStream.range(0, 10_000).mapToObj(i -> "next-order-" + i)
                    .filter(id -> partitions.getPartition(id).getPartitionId() == partition)
                    .findFirst().orElseThrow();
            orders.put(nextOrderId, firstOrder);
            awaitValue(work, new ItemKey(nextOrderId, "line-1"), new ItemTask(originalLine, 1));

            assertNull(work.get(firstTask));
        } finally {
            member.shutdown();
        }
    }

    @Test
    void emitsOnlyAfterEveryDistinctLineArrives() {
        var state = new GatherState();
        var second = new ItemResult(new ItemKey("order-1", "line-2"), 2, REJECTED);
        var first = new ItemResult(new ItemKey("order-1", "line-1"), 2, ACCEPTED);

        assertNull(state.add("order-1", second));
        assertNull(state.add("order-1", second));
        assertFalse(state.isComplete());

        var result = state.add("order-1", first);
        assertEquals(Map.entry("order-1", new OrderResult(Map.of("line-1", ACCEPTED, "line-2", REJECTED))), result);
        assertTrue(state.isComplete());
    }

    @Test
    void rejectsConflictingExpectedCounts() {
        var state = new GatherState();
        state.add("order-1", new ItemResult(new ItemKey("order-1", "line-1"), 2, ACCEPTED));

        assertThrows(IllegalArgumentException.class,
                () -> state.add("order-1", new ItemResult(new ItemKey("order-1", "line-2"), 3, ACCEPTED)));
    }

    @Test
    void rejectsEmptyOrdersAndDuplicateLineIds() {
        assertThrows(IllegalArgumentException.class, () -> new Order(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new Order(List.of(
                new LineItem("line-1", "book", 2), new LineItem("line-1", "pen", 1))));
    }
}
