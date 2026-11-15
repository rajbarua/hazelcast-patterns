package com.hz.patterns.locality;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.map.IMap;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(90)
class ComputeOnDataTest {
    private static LocalCluster cluster;

    @BeforeAll
    static void startMembersAndClient() throws InterruptedException {
        cluster = LocalCluster.start();
    }

    @AfterAll
    static void stopMembersAndClient() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private IMap<StockKey, Integer> newMap() {
        return cluster.client().getMap("stock-" + UUID.randomUUID());
    }

    @Test
    void testJvmGrantsHazelcastAccessToJavaInternals() {
        Module hazelcast = Hazelcast.class.getModule();
        Module javaBase = Object.class.getModule();
        Module javaManagement = ManagementFactory.class.getModule();
        Module jdkManagement = com.sun.management.OperatingSystemMXBean.class.getModule();

        assertAll(
                () -> assertTrue(javaBase.isExported("jdk.internal.ref", hazelcast), "jdk.internal.ref export"),
                () -> assertTrue(javaBase.isOpen("java.lang", hazelcast), "java.lang access"),
                () -> assertTrue(javaBase.isOpen("sun.nio.ch", hazelcast), "sun.nio.ch access"),
                () -> assertTrue(javaManagement.isOpen("sun.management", hazelcast), "sun.management access"),
                () -> assertTrue(jdkManagement.isOpen("com.sun.management.internal", hazelcast),
                        "com.sun.management.internal access"));
    }

    @Test
    void relatedKeysShareAPartitionButRemainDistinctEntries() {
        var map = newMap();
        var book = new StockKey("warehouse-a", "book");
        var pen = new StockKey("warehouse-a", "pen");
        map.put(book, 10);
        map.put(pen, 20);
        assertNotEquals(book, pen);
        assertEquals(cluster.partitionOf(book), cluster.partitionOf(pen));
        assertEquals(cluster.ownerOf(book), cluster.ownerOf(pen));
        assertEquals(10, map.get(book));
        assertEquals(20, map.get(pen));
        var remote = ComputeOnData.differentOwnerKey(cluster, book);
        assertNotEquals(cluster.ownerOf(book), cluster.ownerOf(remote));
    }

    @Test
    void processorRunsOnOwnerAndMatchesUncontendedClientUpdate() {
        var clientMap = newMap();
        var ownerMap = newMap();
        var key = new StockKey("warehouse-a", "book");
        clientMap.put(key, 10);
        ownerMap.put(key, 10);
        clientMap.put(key, clientMap.get(key) - 3);
        UUID owner = cluster.ownerOf(key).getCluster().getLocalMember().getUuid();
        Reservation result = ownerMap.executeOnKey(key, new ReserveStock(3));
        assertTrue(result.accepted());
        assertEquals(10, result.before());
        assertEquals(7, result.after());
        assertEquals(owner, result.executedOn());
        assertEquals(clientMap.get(key), ownerMap.get(key));
    }

    @Test
    void separateReadsAndWritesCanLoseAReservation() {
        var map = newMap();
        var key = new StockKey("warehouse-a", "book");
        map.put(key, 1);
        // A deterministic interleaving: both callers read before either writes.
        int callerA = map.get(key);
        int callerB = map.get(key);
        assertTrue(callerA >= 1 && callerB >= 1);
        map.put(key, callerA - 1);
        map.put(key, callerB - 1);
        assertEquals(0, map.get(key)); // Two callers accepted the same single item.
    }

    @Test
    void concurrentProcessorsCannotReserveMoreThanAvailable() throws Exception {
        var map = newMap();
        var key = new StockKey("warehouse-a", "book");
        map.put(key, 10);
        List<CompletableFuture<Reservation>> requests = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            requests.add(map.submitToKey(key, new ReserveStock(1)).toCompletableFuture());
        }
        CompletableFuture.allOf(requests.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        var results = requests.stream().map(CompletableFuture::join).toList();
        assertEquals(10, results.stream().filter(Reservation::accepted).count());
        assertEquals(20, results.stream().filter(r -> !r.accepted()).count());
        assertTrue(results.stream().allMatch(r -> r.after() >= 0));
        assertEquals(0, map.get(key));
    }

    @Test
    void missingOrInsufficientStockIsNotChanged() {
        var map = newMap();
        var key = new StockKey("warehouse-a", "book");
        var missing = map.executeOnKey(key, new ReserveStock(1));
        assertFalse(missing.accepted());
        assertNull(missing.before());
        assertNull(missing.after());
        assertFalse(map.containsKey(key));
        map.put(key, 2);
        var rejected = map.executeOnKey(key, new ReserveStock(3));
        assertFalse(rejected.accepted());
        assertEquals(2, rejected.after());
        assertEquals(2, map.get(key));
        var exact = map.executeOnKey(key, new ReserveStock(2));
        assertTrue(exact.accepted());
        assertEquals(0, map.get(key));
    }

    @Test
    void quantityMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new ReserveStock(0));
        assertThrows(IllegalArgumentException.class, () -> new ReserveStock(-1));
    }

    @Test
    void reservationFollowsDataAfterOwnerLeaves() throws InterruptedException {
        // Use a separate cluster so this membership change cannot affect other tests.
        try (LocalCluster moving = LocalCluster.start()) {
            IMap<StockKey, Integer> map = moving.client().getMap("moving-stock");
            var key = new StockKey("warehouse-a", "book");
            map.put(key, 10);
            var before = map.executeOnKey(key, new ReserveStock(3));
            assertTrue(before.accepted());
            assertEquals(moving.ownerOf(key).getCluster().getLocalMember().getUuid(), before.executedOn());
            moving.stopOwnerOf(key);
            UUID newOwner = moving.ownerOf(key).getCluster().getLocalMember().getUuid();
            assertNotEquals(before.executedOn(), newOwner);
            var after = map.executeOnKey(key, new ReserveStock(2));
            assertTrue(after.accepted());
            assertEquals(7, after.before());
            assertEquals(5, after.after());
            assertEquals(newOwner, after.executedOn());
            assertEquals(5, map.get(key));
        }
    }
}
