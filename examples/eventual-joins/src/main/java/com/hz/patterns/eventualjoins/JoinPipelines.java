package com.hz.patterns.eventualjoins;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hz.patterns.eventualjoins.ProductJoinState.Event;
import com.hz.patterns.eventualjoins.ProductJoinState.OrderArrived;
import com.hz.patterns.eventualjoins.ProductJoinState.ProductArrived;
import com.hz.patterns.eventualjoins.ProductLookup.RetryPolicy;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hz.patterns.eventualjoins.EventualJoins.ORDERS;
import static com.hz.patterns.eventualjoins.EventualJoins.PRODUCTS;
import static com.hz.patterns.eventualjoins.EventualJoins.RESULTS;
import static com.hz.patterns.eventualjoins.EventualJoins.STREAM_RESULTS;

final class JoinPipelines {
    private JoinPipelines() {
    }

    static Pipeline lookupWithRetry(RetryPolicy policy) {
        var lookups = ServiceFactories.sharedService(
                context -> new ProductLookup(context.hazelcastInstance().getMap(PRODUCTS), policy),
                ProductLookup::close);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<String, String>mapJournal(ORDERS, START_FROM_OLDEST))
                .withoutTimestamps()
                // Pending retries occupy a slot, but completed lookups can emit in any order.
                .mapUsingServiceAsync(lookups, 8, false, (lookup, order) ->
                        lookup.find(order.getValue()).thenApply(result -> entry(order.getKey(), result)))
                .writeTo(Sinks.map(RESULTS));
        return pipeline;
    }

    static Pipeline statefulStreamJoin() {
        Pipeline pipeline = Pipeline.create();
        StreamStage<Event> orders = pipeline.readFrom(Sources.<String, String>mapJournal(ORDERS, START_FROM_OLDEST))
                .withoutTimestamps()
                .map(order -> new OrderArrived(order.getValue(), order.getKey()));
        StreamStage<Event> products = pipeline.readFrom(Sources.<String, String>mapJournal(PRODUCTS, START_FROM_OLDEST))
                .withoutTimestamps()
                .map(product -> new ProductArrived(product.getKey(), product.getValue()));

        orders.merge(products)
                .groupingKey(Event::productId)
                // An unmatched event updates the state and emits nothing until a match arrives.
                .mapStateful(ProductJoinState::new, (state, productId, event) -> state.accept(event))
                // A single product event can complete several orders because they were waiting for the product.
                .flatMap(Traversers::traverseIterable)
                .writeTo(Sinks.map(STREAM_RESULTS));
        return pipeline;
    }
}
