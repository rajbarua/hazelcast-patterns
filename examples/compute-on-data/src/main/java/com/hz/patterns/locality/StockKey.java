package com.hz.patterns.locality;

import com.hazelcast.partition.PartitionAware;
import java.io.Serializable;
import java.util.Objects;

public record StockKey(String warehouse, String product)
        implements PartitionAware<String>, Serializable {
    public StockKey {
        Objects.requireNonNull(warehouse, "warehouse");
        Objects.requireNonNull(product, "product");
    }

    @Override
    public String getPartitionKey() {
        return warehouse;
    }
}
