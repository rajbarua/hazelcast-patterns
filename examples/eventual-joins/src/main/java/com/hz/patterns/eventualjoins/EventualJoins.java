package com.hz.patterns.eventualjoins;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.map.IMap;
import com.hz.patterns.eventualjoins.ProductLookup.RetryPolicy;

import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;

public final class EventualJoins {
    static final String ORDERS = "orders";
    static final String PRODUCTS = "products";
    static final String RESULTS = "order-enrichment";
    static final String STREAM_RESULTS = "stream-order-enrichment";

    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance member = startMember();
        try {
            IMap<String, String> orders = member.getMap(ORDERS);
            IMap<String, String> products = member.getMap(PRODUCTS);
            IMap<String, JoinResult> lookupJoinResults = member.getMap(RESULTS);
            IMap<String, JoinResult> streamJoinResults = member.getMap(STREAM_RESULTS);
            Job lookupJob = member.getJet().newJob(JoinPipelines.lookupWithRetry(new RetryPolicy(7, 100, 800)));
            try {
                Job streamJob = member.getJet().newJob(JoinPipelines.statefulStreamJoin());
                try {
                    //just adding orders will make both jobs wait for the corresponding product.
                    orders.put("order-1", "book");
                    //Optional call. Prove that the lookup job retries at least once. This call returns after the first lookup attempt has been made.
                    await("first product lookup", () -> products.getLocalMapStats().getGetOperationCount() > 0);
                    //putting the product will allow both jobs to complete the join.
                    products.put("book", "Notebook");
                    printLookupJoinResult(lookupJoinResults, "order-1");
                    printStreamJoinResult(streamJoinResults, "order-1");
                    //add another order but don't add the product yet. The lookup job will retry until it exhausts its attempts.
                    orders.put("order-2", "pen");
                    printLookupJoinResult(lookupJoinResults, "order-2");
                    // Deliver the product after the lookup job has exhausted its attempts.
                    products.put("pen", "Fountain pen");
                    printStreamJoinResult(streamJoinResults, "order-2");
                } finally {
                    streamJob.cancel();
                }
            } finally {
                lookupJob.cancel();
            }
        } finally {
            member.shutdown();
        }
    }

    static HazelcastInstance startMember() {
        Config config = new Config().setClusterName("eventual-joins-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getMapConfig(ORDERS).getEventJournalConfig().setEnabled(true);
        config.getMapConfig(PRODUCTS).getEventJournalConfig().setEnabled(true);

        var network = config.getNetworkConfig().setPort(0);
        network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        network.getJoin().getAutoDetectionConfig().setEnabled(false);
        network.getJoin().getMulticastConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void printLookupJoinResult(IMap<String, JoinResult> results, String orderId) throws InterruptedException {
        // Wait till results have the orderId key.
        await("result for " + orderId, () -> results.containsKey(orderId));
        JoinResult result = results.get(orderId);
        String outcome = result.productName() == null ? "PRODUCT_MISSING" : "JOINED " + result.productName();
        System.out.printf("Lookup %s: %s after %d attempts%n", orderId, outcome, result.attempts());
    }

    private static void printStreamJoinResult(IMap<String, JoinResult> results, String orderId) throws InterruptedException {
        // Wait till results have the orderId key. 
        await("stream join result for " + orderId, () -> results.containsKey(orderId));
        System.out.printf("Stream %s: JOINED %s%n", orderId, results.get(orderId).productName());
    }

    /**
     * Waits for a condition to become true, throwing an exception if it does not happen within 20 seconds.
     */
    static void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Timed out waiting for " + description);
            }
            Thread.sleep(20);
        }
    }
}
