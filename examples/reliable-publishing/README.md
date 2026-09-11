# Reliable publishing

**Status: planned.** There is no runnable code yet.

Creating an outgoing message and delivering it are separate steps. This example will explore the handoff between them and the evidence needed to know what was sent.

## What you will learn

- How stable output IDs identify the same message across retries.
- How a pending record connects output creation to publishing.
- Where transaction and recovery guarantees begin and end.
- How committed output and durable records help verify delivery.

## What you will be able to observe

You will be able to follow restarts around delivery and commit boundaries, and examine missing or duplicate outputs within the stated guarantee.

## Planned setup

Test-managed members, Kafka and a database. The exact versions and edition requirements are not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
