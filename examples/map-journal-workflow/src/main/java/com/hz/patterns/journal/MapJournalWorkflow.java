package com.hz.patterns.journal;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import java.time.Duration;
import java.util.UUID;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

public final class MapJournalWorkflow {
    static final String ORDERS = "orders";
    static final String VALIDATION = "order-validation";

    enum Validation {
        ACCEPTED, REJECTED
    }

    private MapJournalWorkflow() {
    }

    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance member = startMember();
        try {
            IMap<String, Integer> orders = member.getMap(ORDERS);
            IMap<String, Validation> results = member.getMap(VALIDATION);

            // Seed before submission so the first result demonstrates reading retained journal events.
            orders.put("order-1", 3);
            Job job = member.getJet().newJob(validationPipeline());
            try {
                awaitResult(results, "order-1", Validation.ACCEPTED);
                System.out.println("order-1 (quantity 3): " + results.get("order-1"));

                orders.put("order-2", 0);
                awaitResult(results, "order-2", Validation.REJECTED);
                System.out.println("order-2 (quantity 0): " + results.get("order-2"));

                orders.put("order-2", 2);
                awaitResult(results, "order-2", Validation.ACCEPTED);
                System.out.println("order-2 (quantity corrected to 2): " + results.get("order-2"));
            } finally {
                job.cancel();
            }
        } finally {
            member.shutdown();
        }
    }

    static HazelcastInstance startMember() {
        Config config = new Config().setClusterName("map-journal-workflow-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMapConfig(ORDERS).getEventJournalConfig().setEnabled(true);

        // Keep discovery and traffic local to this embedded member.
        var network = config.getNetworkConfig().setPort(0);
        network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        network.getJoin().getAutoDetectionConfig().setEnabled(false);
        network.getJoin().getMulticastConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    static Pipeline validationPipeline() {
        Pipeline pipeline = Pipeline.create();
        // The default journal source emits the key and new value for adds and updates.
        pipeline.readFrom(Sources.<String, Integer>mapJournal(ORDERS, START_FROM_OLDEST))
                .withoutTimestamps()
                .map(order -> entry(order.getKey(), order.getValue() > 0 ? Validation.ACCEPTED : Validation.REJECTED))
                .writeTo(Sinks.map(VALIDATION));
        return pipeline;
    }

    static void awaitResult(IMap<String, Validation> results, String orderId, Validation expected)
            throws InterruptedException {
        // Jet writes asynchronously, so poll for the expected result with a timeout.
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            if (results.get(orderId) == expected) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Timed out waiting for " + orderId + " to become " + expected
                + ". Last result: " + results.get(orderId));
    }
}
