# Async batched I/O

**Status: planned.** There is no runnable code yet.

A pipeline calling another service needs to balance throughput with the number of requests in flight. This example will explore batching and bounded asynchronous calls.

## What you will learn

- How several inputs share one batch request.
- How a concurrency limit controls requests in flight.
- How slow responses and partial failures affect the batch results.

## What you will be able to observe

You will be able to inspect concurrent calls and match each response or failure to its original input.

## Planned setup

A Hazelcast member and a small test service. The required version and edition are not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
