# Resilient order flow

**Status: planned.** There is no runnable code yet.

A small order workflow will combine validation, item aggregation, recovery and notification delivery.

## Patterns you will follow

- [Map journal workflow](../../examples/map-journal-workflow/README.md) for stage transitions.
- [Compute with data](../../examples/compute-with-data/README.md) for work near its state.
- [Scatter/gather](../../examples/scatter-gather/README.md) for item completion.
- [Eventual joins](../../examples/eventual-joins/README.md) for late dependencies.
- [Snapshot state](../../examples/snapshot-state/README.md) and [journal recovery](../../examples/journal-recovery/README.md) for recovery.
- [Reliable publishing](../../examples/reliable-publishing/README.md) for outgoing notifications.

## What you will be able to observe

You will be able to follow an order from arrival through notification, then explore duplicate items, a delayed lookup record and a member restart. The example will show how each case affects the final result and the intermediate stages.

You will also see where a guarantee from one stage does or does not carry across a job or service boundary.

## Planned setup

You will be able to run this flow without starting the smaller examples. The exact dependencies and run commands are not available yet.

[Repository home](../../README.md)
