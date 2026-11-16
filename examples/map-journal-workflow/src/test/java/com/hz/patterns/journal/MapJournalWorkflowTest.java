package com.hz.patterns.journal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.hz.patterns.journal.MapJournalWorkflow.ORDERS;
import static com.hz.patterns.journal.MapJournalWorkflow.VALIDATION;
import static com.hz.patterns.journal.MapJournalWorkflow.Validation.ACCEPTED;
import static com.hz.patterns.journal.MapJournalWorkflow.Validation.REJECTED;
import static com.hz.patterns.journal.MapJournalWorkflow.awaitResult;
import static com.hz.patterns.journal.MapJournalWorkflow.validationPipeline;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Timeout(60)
class MapJournalWorkflowTest {
    private HazelcastInstance member;
    private IMap<String, Integer> orders;
    private IMap<String, MapJournalWorkflow.Validation> results;

    @BeforeEach
    void startMember() {
        member = MapJournalWorkflow.startMember();
        orders = member.getMap(ORDERS);
        results = member.getMap(VALIDATION);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void validatesRetainedAddsEvenWhenTheEntryHasBeenRemoved() throws InterruptedException {
        orders.put("removed-order", 3);
        orders.remove("removed-order");
        member.getJet().newJob(validationPipeline());

        awaitResult(results, "removed-order", ACCEPTED);

        assertFalse(orders.containsKey("removed-order"));
        assertEquals(Map.of("removed-order", ACCEPTED).entrySet(), results.entrySet());
    }

    @Test
    void rejectsInvalidQuantityAndUpdatesStatusWhenCorrected() throws InterruptedException {
        orders.put("order-1", 3);
        member.getJet().newJob(validationPipeline());
        awaitResult(results, "order-1", ACCEPTED);

        orders.put("order-2", 0);
        awaitResult(results, "order-2", REJECTED);
        orders.put("order-2", 2);
        awaitResult(results, "order-2", ACCEPTED);

        assertEquals(Map.of("order-1", ACCEPTED, "order-2", ACCEPTED).entrySet(), results.entrySet());
        assertEquals(Map.of("order-1", 3, "order-2", 2).entrySet(), orders.entrySet());
    }
}
