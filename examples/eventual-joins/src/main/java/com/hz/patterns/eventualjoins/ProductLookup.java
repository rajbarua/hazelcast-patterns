package com.hz.patterns.eventualjoins;

import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class ProductLookup implements AutoCloseable {
    // policy that controls the retry behaviour
    record RetryPolicy(int maxAttempts, long initialDelayMillis, long maxDelayMillis) implements Serializable {
        RetryPolicy {
            if (maxAttempts < 1 || initialDelayMillis < 1 || maxDelayMillis < initialDelayMillis) {
                throw new IllegalArgumentException("Retry policy requires attempts >= 1 and 0 < initial delay <= max delay");
            }
        }
    }

    private final IMap<String, String> products;
    private final RetryPolicy policy;
    
    //will be used for retrying after a failed lookup. Controlled by RetryPolicy
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().daemon().name("product-lookup-retries").factory());
    //tracks the pending lookups so that we can complete them exceptionally when the lookup is closed.
    private final Set<CompletableFuture<JoinResult>> pending = new HashSet<>();
    private boolean closed;

    ProductLookup(IMap<String, String> products, RetryPolicy policy) {
        this.products = products;
        this.policy = policy;
    }

    /**
     * Finds the product with the given ID. 
     * The returned future will complete with a {@link JoinResult} containing the product value and the number of attempts made. 
     */
    synchronized CompletableFuture<JoinResult> find(String productId) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("Product lookup is closed"));
        }

        var result = new CompletableFuture<JoinResult>();
        pending.add(result);
        result.whenComplete((value, failure) -> {
            synchronized (this) {
                pending.remove(result);
            }
        });
        read(productId, result, 1, policy.initialDelayMillis());
        return result;
    }

    /**
     * Reads the product from the map asynchronously. 
     */
    private synchronized void read(String productId, CompletableFuture<JoinResult> futureResult,
                                   int attempt, long delayMillis) {
        if (closed || futureResult.isDone()) {
            return;
        }

        try {
            //read from IMap asynchronously and then complete the future.
            products.getAsync(productId).whenComplete((product, failure) ->
                    finishAttempt(productId, futureResult, attempt, delayMillis, product, failure));
        } catch (RuntimeException failure) {
            futureResult.completeExceptionally(failure);
        }
    }

    /**
     * Completes the current attempt to find the product.
     */
    private synchronized void finishAttempt(String productId, CompletableFuture<JoinResult> futureResult,
                                            int attempt, long delayMillis, String product, Throwable failure) {
        if (closed || futureResult.isDone()) {
            return;
        }

        if (failure != null) {
            //upon previous attempt failed with error, complete the future exceptionally.
            futureResult.completeExceptionally(failure);
        } else if (product != null || attempt == policy.maxAttempts()) {
            //either the product was found or we have exhausted the attempts, complete the future with the result.
            futureResult.complete(new JoinResult(product, attempt));
        } else {
            // The product was not found, but we can retry. Schedule the next attempt with an increased delay.
            // Double the delay up to the cap without overflowing a long.
            long nextDelay = delayMillis + Math.min(delayMillis, policy.maxDelayMillis() - delayMillis);
            scheduler.schedule(() -> read(productId, futureResult, attempt + 1, nextDelay),
                    delayMillis, TimeUnit.MILLISECONDS);
        }
    }


    @Override
    public void close() {
        List<CompletableFuture<JoinResult>> outstanding;
        synchronized (this) {
            closed = true;
            scheduler.shutdownNow();
            outstanding = List.copyOf(pending);
            pending.clear();
        }
        // Tasks removed from the timer queue still have futures Jet may be waiting on.
        outstanding.forEach(result -> result.completeExceptionally(new IllegalStateException("Product lookup is closed")));
    }
}
