package com.hz.patterns.locality;

import com.hazelcast.map.IMap;

public final class ComputeOnData {
    private ComputeOnData() {
    }

    public static void main(String[] args) throws InterruptedException {
        try (LocalCluster cluster = LocalCluster.start()) {
            // Add two products to the same warehouse to show that they are routed to the same partition and owner.
            StockKey books = new StockKey("warehouse-a", "book");
            StockKey pens = new StockKey("warehouse-a", "pen");
            System.out.printf("book partition=%d, pen partition=%d (same warehouse)%n",
                    cluster.partitionOf(books), cluster.partitionOf(pens));

            // Give both approaches the same starting stock so their updates are independent.
            IMap<StockKey, Integer> clientStock = cluster.client().getMap("client-stock");
            IMap<StockKey, Integer> ownerStock = cluster.client().getMap("owner-stock");
            clientStock.put(books, 10);
            ownerStock.put(books, 10);

            // Following lines could result in a race condition if another client is also updating the same entry.
            int available = clientStock.get(books);
            if (available >= 3) {
                clientStock.put(books, available - 3);
            }
            System.out.println("Client get/put: 10 -> " + clientStock.get(books));

            // Alternatively execute the same logic on the member that owns the entry, which avoids a remote lookup and is atomic.
            // Lets get the owner before the operation so we can show that it is executed on the same member.
            var ownerBefore = cluster.ownerOf(books).getCluster().getLocalMember().getUuid();
            Reservation result = ownerStock.executeOnKey(books, new ReserveStock(3));
            System.out.printf("EntryProcessor: %d -> %d, accepted=%s%n",
                    result.before(), result.after(), result.accepted());
            System.out.printf("Owner=%s%nExecuted on=%s%n", ownerBefore, result.executedOn());

            // Show where another warehouse would be routed by inspecting ownership metadata alone.
            StockKey elsewhere = differentOwnerKey(cluster, books);
            System.out.printf("Different warehouse: partition=%d, owner=%s%n",
                    cluster.partitionOf(elsewhere),
                    cluster.ownerOf(elsewhere).getCluster().getLocalMember().getUuid());

            // Now stop the member that owns the books entry to show that ownership can move to another member
            // and that the entry processor still executes correctly on the new owner.
            System.out.println("Stopping the book owner gracefully...");
            cluster.stopOwnerOf(books);
            var ownerAfter = cluster.ownerOf(books).getCluster().getLocalMember().getUuid();
            Reservation afterMove = ownerStock.executeOnKey(books, new ReserveStock(2));
            System.out.printf("New owner=%s%nExecuted on=%s%nStock after move: %d -> %d%n",
                    ownerAfter, afterMove.executedOn(), afterMove.before(), afterMove.after());
        }
    }

    /**
     * Create and return a key that is guaranteed to be owned by a different member than the given key.
     */
    static StockKey differentOwnerKey(LocalCluster cluster, StockKey original) {
        var originalOwner = cluster.ownerOf(original);
        // A different warehouse can still land on the same member, so check each candidate's owner.
        for (int i = 0; i < 10_000; i++) {
            StockKey candidate = new StockKey("other-warehouse-" + i, "book");
            if (cluster.ownerOf(candidate) != originalOwner) {
                return candidate;
            }
        }
        throw new IllegalStateException("Could not find a key on a different member");
    }
}
