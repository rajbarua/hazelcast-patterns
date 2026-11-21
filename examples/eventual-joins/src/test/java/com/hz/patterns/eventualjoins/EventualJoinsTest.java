package com.hz.patterns.eventualjoins;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.map.IMap;
import com.hz.patterns.eventualjoins.ProductLookup.RetryPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static com.hz.patterns.eventualjoins.EventualJoins.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

class EventualJoinsTest {
    private HazelcastInstance member;
    private IMap<String, String> orders;
    private IMap<String, String> products;
    private IMap<String, JoinResult> results;
    private Job job;

    @BeforeEach
    void startMember() {
        member = EventualJoins.startMember();
        orders = member.getMap(ORDERS);
        products = member.getMap(PRODUCTS);
        results = member.getMap(RESULTS);
    }

    @AfterEach
    void stopMember() {
        try {
            if (job != null) {
                job.cancel();
            }
        } finally {
            member.shutdown();
        }
    }

    @Test
    void readyOrderCanFinishWhileAnEarlierOrderWaitsForItsProduct() throws InterruptedException {
        products.put("pen", "Fountain pen");
        job = member.getJet().newJob(JoinPipelines.lookupWithRetry(new RetryPolicy(20, 100, 800)));
        orders.put("late", "book");
        await("first lookup for the missing book", () -> products.getLocalMapStats().getGetOperationCount() > 0);

        orders.put("ready", "pen");
        assertEquals(new JoinResult("Fountain pen", 1), resultFor("ready"));
        assertNull(results.get("late"));

        products.put("book", "Notebook");
        JoinResult joined = resultFor("late");
        assertEquals("Notebook", joined.productName());
        assertTrue(joined.attempts() > 1);
        assertEquals(2, results.size());
    }

    @Test
    void missingProductEmitsOnceAtTheAttemptLimit() throws InterruptedException {
        job = member.getJet().newJob(JoinPipelines.lookupWithRetry(new RetryPolicy(3, 20, 40)));
        orders.put("missing", "book");

        JoinResult exhausted = new JoinResult(null, 3);
        assertEquals(exhausted, resultFor("missing"));
        assertEquals(3, products.getLocalMapStats().getGetOperationCount());
        assertEquals("book", orders.get("missing"));

        products.put("book", "Notebook");
        orders.put("new-order", "book");
        assertEquals(new JoinResult("Notebook", 1), resultFor("new-order"));
        assertEquals(exhausted, results.get("missing"));
        assertEquals(2, results.size());
    }

    @Test
    void streamJoinCorrelatesBothJournalsWithoutProductLookups() throws InterruptedException {
        results = member.getMap(STREAM_RESULTS);
        orders.put("first-book", "book");
        orders.put("second-book", "book");
        orders.put("unmatched", "unknown");
        products.put("pen", "Fountain pen");

        job = member.getJet().newJob(JoinPipelines.statefulStreamJoin());
        orders.put("first-pen", "pen");
        assertEquals(new JoinResult("Fountain pen", 0), resultFor("first-pen"));
        assertNull(results.get("first-book"));
        assertNull(results.get("second-book"));

        products.put("book", "Notebook");
        assertEquals(new JoinResult("Notebook", 0), resultFor("first-book"));
        assertEquals(new JoinResult("Notebook", 0), resultFor("second-book"));

        orders.put("third-book", "book");
        assertEquals(new JoinResult("Notebook", 0), resultFor("third-book"));
        assertNull(results.get("unmatched"));
        assertEquals(4, results.size());
        assertEquals(0, products.getLocalMapStats().getGetOperationCount());
    }

    @Test
    void closingTheServiceSettlesPendingWork() throws Exception {
        var lookup = new ProductLookup(products, new RetryPolicy(2, 30_000, 30_000));
        try (lookup) {
            var pending = lookup.find("book");
            var cancelled = lookup.find("pen");
            await("both lookups before shutdown", () -> products.getLocalMapStats().getGetOperationCount() == 2);
            assertTrue(cancelled.cancel(false));
            assertFalse(pending.isDone());

            lookup.close();

            var failure = assertThrows(ExecutionException.class, () -> pending.get(1, SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertTrue(cancelled.isCancelled());
            assertTrue(lookup.find("book").isCompletedExceptionally());
            assertEquals(2, products.getLocalMapStats().getGetOperationCount());
        }
    }

    private JoinResult resultFor(String orderId) throws InterruptedException {
        await("result for " + orderId, () -> results.containsKey(orderId));
        return results.get(orderId);
    }
}
