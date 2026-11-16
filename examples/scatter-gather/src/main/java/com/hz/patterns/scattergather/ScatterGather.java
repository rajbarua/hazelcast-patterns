package com.hz.patterns.scattergather;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hz.patterns.scattergather.ScatterGather.LineStatus.ACCEPTED;
import static com.hz.patterns.scattergather.ScatterGather.LineStatus.REJECTED;

public final class ScatterGather {
    static final String ORDERS = "orders";
    static final String ITEM_WORK = "item-work";

    record Order(List<LineItem> items, OrderResult result) implements Serializable {
        Order(List<LineItem> items) {
            this(items, null);
        }

        Order {
            items = List.copyOf(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("An order must contain at least one line");
            }
            if (items.stream().map(LineItem::itemId).distinct().count() != items.size()) {
                throw new IllegalArgumentException("Item IDs must be unique within an order");
            }
        }
    }

    record LineItem(String itemId, String product, int quantity) implements Serializable {}
    record ItemKey(String orderId, String itemId) implements Serializable {}
    record ItemTask(LineItem item, int expectedCount) implements Serializable {}
    record ItemResult(ItemKey key, int expectedCount, LineStatus status) implements Serializable {}

    enum LineStatus {
        ACCEPTED, REJECTED
    }

    record OrderResult(Map<String, LineStatus> lines) implements Serializable {
        OrderResult {
            lines = Map.copyOf(lines);
        }

        LineStatus status() {
            return lines.containsValue(REJECTED) ? REJECTED : ACCEPTED;
        }
    }

    private ScatterGather() {
    }

    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance member = startMember();
        try {
            IMap<String, Order> orders = member.getMap(ORDERS);
            Job gather = member.getJet().newJob(gatherPipeline());
            Job scatter = member.getJet().newJob(scatterPipeline());
            try {
                // Submit two orders, one with all lines accepted, the other with one line rejected.
                List<LineItem> first = List.of(new LineItem("line-1", "book", 2), 
                                                new LineItem("line-2", "pen", 1));
                orders.put("order-1", new Order(first));
                print("order-1", awaitValue(orders, "order-1", new Order(first,
                        new OrderResult(Map.of("line-1", ACCEPTED, "line-2", ACCEPTED)))).result());

                List<LineItem> second = List.of(new LineItem("line-1", "book", 2), 
                                                new LineItem("line-2", "pen", 0));
                orders.put("order-2", new Order(second));
                print("order-2", awaitValue(orders, "order-2", new Order(second,
                        new OrderResult(Map.of("line-1", ACCEPTED, "line-2", REJECTED)))).result());
            } finally {
                scatter.cancel();
                gather.cancel();
            }
        } finally {
            member.shutdown();
        }
    }

    static Pipeline scatterPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<String, Order>mapJournal(ORDERS, START_FROM_OLDEST))
                .withoutTimestamps()
                // The gather job also writes to orders, so dispatch only pending orders.
                .filter(order -> order.getValue().result() == null)
                .flatMap(order -> {
                    List<LineItem> items = order.getValue().items();
                    int expectedCount = items.size();
                    return traverseIterable(items).map(item -> entry(
                            new ItemKey(order.getKey(), item.itemId()), new ItemTask(item, expectedCount)));
                })
                .writeTo(Sinks.map(ITEM_WORK));
        return pipeline;
    }

    static Pipeline gatherPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<ItemKey, ItemTask>mapJournal(ITEM_WORK, START_FROM_OLDEST))
                .withoutTimestamps()
                .map(task -> new ItemResult(task.getKey(), task.getValue().expectedCount(),
                        task.getValue().item().quantity() > 0 ? ACCEPTED : REJECTED))
                .groupingKey(result -> result.key().orderId())
                // Emit on completion, then discard that order's accumulator.
                .mapStateful(0, GatherState::new, GatherState::add,
                        (state, orderId, result) -> state.isComplete(), null)
                .writeTo(Sinks.<Map.Entry<String, OrderResult>, String, Order, Void>mapWithEntryProcessor(
                        ORDERS, Map.Entry::getKey, completed -> {
                            OrderResult result = completed.getValue();
                            return orderEntry -> {
                                Order order = orderEntry.getValue();
                                if (order != null && order.result() == null) {
                                    orderEntry.setValue(new Order(order.items(), result));
                                }
                                return null;
                            };
                        }));
        return pipeline;
    }

    static HazelcastInstance startMember() {
        Config config = new Config().setClusterName("scatter-gather-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMapConfig(ORDERS).getEventJournalConfig().setEnabled(true);
        config.getMapConfig(ITEM_WORK).getEventJournalConfig().setEnabled(true);

        var network = config.getNetworkConfig().setPort(0);
        network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        network.getJoin().getAutoDetectionConfig().setEnabled(false);
        network.getJoin().getMulticastConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    static <K, V> V awaitValue(IMap<K, V> map, K key, V expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            V observed = map.get(key);
            if (expected.equals(observed)) {
                return observed;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Timed out waiting for " + map.getName() + "[" + key + "] to reach "
                + expected + ". Last value: " + map.get(key));
    }

    private static void print(String orderId, OrderResult result) {
        System.out.printf("%s: %s after gathering %d lines%n", orderId, result.status(), result.lines().size());
    }
}
