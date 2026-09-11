# Compute with data

**Status: planned.** There is no runnable code yet.

How can code run beside its data instead of fetching that data to a client? This example will explore local computation using a small stock update.

## What you will learn

- How a shared partition key places related records together.
- How client fetch/compute/write differs from an EntryProcessor.
- How to identify the member that owns the data and the member running the code.
- When a lookup or shuffle still needs the network.

## What you will be able to observe

You will be able to compare the stock update result and execution location, then see how ownership changes when data moves between members.

## Planned setup

Multiple members started and stopped by tests. The intended setup uses OSS features; the exact dependency version is not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
