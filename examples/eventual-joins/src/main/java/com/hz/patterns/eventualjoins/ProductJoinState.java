package com.hz.patterns.eventualjoins;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;

final class ProductJoinState implements Serializable {
    sealed interface Event extends Serializable permits OrderArrived, ProductArrived {
        String productId();
    }

    record OrderArrived(String productId, String orderId) implements Event {
    }

    record ProductArrived(String productId, String productName) implements Event {
    }

    private final Set<String> waitingOrders = new LinkedHashSet<>();
    private String productName;

    /**
     * Accepts an event and returns a list of completed orders, if any. 
     * Return list is null if the event was a product arrival with no waiting orders. 
     * Return list is null if the event was an order arrival that is now waiting for its product.
     */
    List<Entry<String, JoinResult>> accept(Event event) {
        return switch (event) {
            case OrderArrived order -> onOrder(order.orderId());
            case ProductArrived product -> onProduct(product.productName());
        };
    }

    /**
     * If an order event arrives and the product is already known, return a completed order. 
     * If the product is not yet known, add the order to the waiting list and return null.
     */
    private List<Entry<String, JoinResult>> onOrder(String orderId) {
        if (productName != null) {
            return List.of(entry(orderId, new JoinResult(productName, 0)));
        }
        waitingOrders.add(orderId);
        return null;
    }

    /**
     * If a product event arrives and there are waiting orders, return a list of completed orders.
     * If there are no waiting orders, just store the product name for future orders and return null.
     */
    private List<Entry<String, JoinResult>> onProduct(String name) {
        productName = name;
        if (waitingOrders.isEmpty()) {
            return null;
        }
        // Keep the product available for later orders and release the completed IDs.
        var result = new JoinResult(productName, 0);
        var completed = waitingOrders.stream().map(orderId -> entry(orderId, result)).toList();
        waitingOrders.clear();
        return completed;
    }
}
