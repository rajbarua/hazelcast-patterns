package com.hz.patterns.locality;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Local setup shared by the walkthrough and its tests. */
public final class LocalCluster implements AutoCloseable {
    private final List<HazelcastInstance> members = new ArrayList<>();
    private HazelcastInstance client;

    private LocalCluster() {
    }

    public static LocalCluster start() throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        // A unique name keeps concurrent runs in separate clusters.
        String name = "compute-on-data-" + UUID.randomUUID();
        try {
            for (int i = 0; i < 3; i++) {
                Config config = new Config().setClusterName(name);
                config.setProperty("hazelcast.phone.home.enabled", "false");
                config.setProperty("hazelcast.logging.type", "none");
                // Small thread pools and partition count keep a three-member laptop demo light.
                config.setProperty("hazelcast.partition.count", "31");
                config.setProperty("hazelcast.operation.thread.count", "2");
                config.setProperty("hazelcast.operation.generic.thread.count", "2");
                config.setProperty("hazelcast.io.input.thread.count", "1");
                config.setProperty("hazelcast.io.output.thread.count", "1");
                config.getMetricsConfig().setEnabled(false);

                // Bind each member to an available loopback port so local runs can coexist.
                var network = config.getNetworkConfig().setPort(0);
                network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");

                // Use the first member's bound address as the TCP seed for subsequent members.
                var join = network.getJoin();
                join.getMulticastConfig().setEnabled(false);
                join.getAutoDetectionConfig().setEnabled(false);
                join.getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(1);
                if (!cluster.members.isEmpty()) {
                    join.getTcpIpConfig().addMember(cluster.address(cluster.members.getFirst()));
                }
                cluster.members.add(Hazelcast.newHazelcastInstance(config));
            }

            // Wait for the initial partition layout and backups before the client submits work.
            cluster.awaitStable();
            ClientConfig clientConfig = new ClientConfig().setClusterName(name);
            clientConfig.setProperty("hazelcast.logging.type", "none");
            clientConfig.getMetricsConfig().setEnabled(false);
            clientConfig.getNetworkConfig().addAddress(cluster.members.stream()
                    .map(cluster::address).toArray(String[]::new));
            clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(15_000);
            cluster.client = HazelcastClient.newHazelcastClient(clientConfig);
            return cluster;
        } catch (RuntimeException | InterruptedException failure) {
            // Startup can fail after some members have already started.
            cluster.close();
            throw failure;
        }
    }

    public HazelcastInstance client() {
        return client;
    }

    /**
     * Return the Hazelcast member that owns the partition for the given key.
     */
    public HazelcastInstance ownerOf(StockKey key) {
        UUID owner = members.getFirst().getPartitionService().getPartition(key).getOwner().getUuid();
        return members.stream().filter(m -> m.getCluster().getLocalMember().getUuid().equals(owner))
                .findFirst().orElseThrow();
    }

    public int partitionOf(StockKey key) {
        return members.getFirst().getPartitionService().getPartition(key).getPartitionId();
    }

    public void stopOwnerOf(StockKey key) throws InterruptedException {
        HazelcastInstance owner = ownerOf(key);
        // A graceful shutdown lets Hazelcast migrate partitions before the owner exits.
        owner.shutdown();
        members.remove(owner);
        awaitStable();
    }

    private void awaitStable() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (System.nanoTime() < deadline) {
            // Membership can converge before partition migration and backup synchronization finish.
            boolean ready = members.stream().allMatch(m ->
                    m.getCluster().getMembers().size() == members.size()
                    && m.getPartitionService().isClusterSafe());
            if (ready) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Cluster did not finish membership changes and backup synchronization");
    }

    private String address(HazelcastInstance member) {
        return "127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort();
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.shutdown();
            }
        } finally {
            // Teardown affects only the members owned by this example.
            members.forEach(m -> m.getLifecycleService().terminate());
        }
    }
}
