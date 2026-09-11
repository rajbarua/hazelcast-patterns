# Learning paths

These paths describe the planned examples. None is runnable yet.

## Understand local compute

1. [Compute with data](../examples/compute-with-data/README.md): find the data owner and run code there.
2. [Map journal workflow](../examples/map-journal-workflow/README.md): react to changes in distributed state.
3. [Scatter/gather](../examples/scatter-gather/README.md): split and combine related work.

## Handle late data and failures

1. [Eventual joins](../examples/eventual-joins/README.md): wait for a dependency that is not visible yet.
2. [Snapshot state](../examples/snapshot-state/README.md): save and restore processing state.
3. [Journal recovery](../examples/journal-recovery/README.md): recover retained state after journal loss.
4. [Durable deduplication, EE](../examples/durable-dedup-ee/README.md): align duplicate checks with durable progress.

## Control throughput and delivery

1. [Async batched I/O](../examples/async-batched-io/README.md): bound calls to another service.
2. [Inter-job backpressure](../examples/inter-job-backpressure/README.md): control work entering a slower workflow.
3. [Reliable publishing](../examples/reliable-publishing/README.md): separate building an output from sending it.

## Combine the patterns

[Resilient order flow](../compositions/resilient-order-flow/README.md) will bring several patterns together. Read the small examples when you want to understand one part in more detail.
