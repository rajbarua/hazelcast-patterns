# Durable deduplication (EE)

**Status: planned.** There is no runnable code yet.

A duplicate check is only useful after a restart if the required record survives. This EE example will explore how durable deduplication relates to snapshot progress.

## What you will learn

- How a retry differs from another attempt with the same business ID.
- How write-behind creates a delay before a deduplication record reaches the database.
- How `EnterpriseSinks.mapFlushSink` fits into the snapshot boundary.

## What you will be able to observe

You will be able to follow a snapshot while persistence is delayed, then examine which deduplication records are available after a restart.

## Planned setup

Hazelcast Enterprise Edition, an evaluation license, test-managed members and a database. You will need a local evaluation license to run it.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
