# Hazelcast Patterns

Small, runnable examples of distributed data and stream processing with Hazelcast.

Each example explains one main idea, shows the code, and tests the result. Related patterns share an example when that makes them easier to understand. Larger examples show how the smaller patterns work together.

**Status:** This repository currently contains the project structure and the first documentation. The examples below are planned. There is no runnable code yet.

## Start here

Start with [Compute with data](examples/compute-with-data/README.md). It will show how running code on the member that owns the data can avoid remote data fetches.

Then follow a [learning path](docs/learning-paths.md), or use the [pattern index](docs/pattern-index.md) to find a specific topic.

## Examples

Every example will have its own build file, README and tests. You will be able to run it from its own folder without building the whole repository or starting another example.

| Example | What you will learn | Run setup |
| --- | --- | --- |
| [Compute with data](examples/compute-with-data/README.md) | Partition keys, data ownership and EntryProcessors | Members started by tests |
| [Map journal workflow](examples/map-journal-workflow/README.md) | React to map updates, enrich records and pass work between stages | Embedded member |
| [Scatter/gather](examples/scatter-gather/README.md) | Split work into items and wait for all expected results | Embedded member |
| [Eventual joins](examples/eventual-joins/README.md) | Handle data that arrives later than the event that needs it | Embedded member |
| [Journal recovery](examples/journal-recovery/README.md) | Detect lost journal events and recover retained state | Members started by tests |
| [Durable deduplication](examples/durable-dedup-ee/README.md) | Keep durable duplicate checks aligned with snapshots | EE, members and database |
| [Async batched I/O](examples/async-batched-io/README.md) | Call a service in batches with bounded concurrency | Member and test service |
| [Inter-job backpressure](examples/inter-job-backpressure/README.md) | Slow ingress when work builds up between jobs | Embedded member |
| [Reliable publishing](examples/reliable-publishing/README.md) | Separate output creation from delivery and verify sent results | Members, Kafka and database |
| [Snapshot state](examples/snapshot-state/README.md) | Serialize processing state and restore it after a restart | Members started by tests |

All entries are planned. Exact Hazelcast versions and any further edition requirements will be documented when each example is implemented.

## Larger examples

[Resilient order flow](compositions/resilient-order-flow/README.md) will combine validation, scatter/gather, recovery and publishing in a small order workflow. It will run on its own and link back to the examples that explain each part.

You will be able to follow an order through several stages and see how a failure in one stage affects the rest of the flow.

## Running examples

There is no root build. Each example will provide its own setup and test commands.

Most examples will start Hazelcast directly from tests. Examples about multiple members will start and stop those members for you. Where a test needs separate processes, Kafka or a database, its README will explain how to start and stop them.

Commands and dependency versions will be added with the working examples. None are provided as runnable instructions yet.

## Open source and Enterprise Edition

Examples marked **EE** will require Hazelcast Enterprise Edition. The other examples will use open-source features where possible. Open-source examples are for education purposes, but for production use we recommend the Enterprise Edition.

Evaluation licenses can be obtained from [Hazelcast.com](https://hazelcast.com/get-started/).

## Repository layout

```text
examples/       Small examples that run independently
compositions/   Larger examples that combine patterns
docs/           Learning paths, pattern index and diagram guide
```

## What you will learn

The examples use familiar models such as orders, stock items and sensor readings, so you can focus on Hazelcast without learning a large business domain first.

You will be able to follow the data in a diagram, read the code that processes it, and use the tests to explore successful results and failures. Each example will explain its guarantees and their limits.

For a suggested reading order, see the [learning paths](docs/learning-paths.md). To explore a particular topic, use the [pattern index](docs/pattern-index.md).
