# Hazelcast Patterns

Small examples of distributed data and stream processing with Hazelcast. Familiar models such as orders and stock items let you focus on the pattern without learning a large business domain.

## Start here

Start with [Compute on data](examples/compute-on-data/README.md#run-it), including its setup and run commands. Each example runs independently; there is no root build. Requirements, diagrams, tests and guarantees live in the example's README.

For a suggested order, follow the [learning paths](docs/learning-paths.md). To find a particular topic, use the [pattern index](docs/pattern-index.md).

## Examples

| Example | Status |
| --- | --- |
| [Compute on data](examples/compute-on-data/README.md) | Runnable |
| [Map journal workflow](examples/map-journal-workflow/README.md) | Runnable |
| [Scatter/gather](examples/scatter-gather/README.md) | Runnable |
| [Eventual joins](examples/eventual-joins/README.md) | Runnable |
| [Journal recovery](examples/journal-recovery/README.md) | Planned |
| [Durable deduplication (EE)](examples/durable-dedup-ee/README.md) | Planned |
| [Async batched I/O](examples/async-batched-io/README.md) | Planned |
| [Inter-job backpressure](examples/inter-job-backpressure/README.md) | Planned |
| [Reliable publishing](examples/reliable-publishing/README.md) | Planned |
| [Snapshot state](examples/snapshot-state/README.md) | Planned |

## Compositions

| Example | Status |
| --- | --- |
| [Resilient order flow](compositions/resilient-order-flow/README.md) | Planned |

Compositions combine several patterns in a larger, independently runnable example.

## Open source and Enterprise Edition

Examples marked **EE** will require Hazelcast Enterprise Edition. The other examples will use open-source features where possible. Open-source examples are for education purposes, but for production use we recommend the Enterprise Edition.

Evaluation licenses can be obtained from [Hazelcast.com](https://hazelcast.com/get-started/).
