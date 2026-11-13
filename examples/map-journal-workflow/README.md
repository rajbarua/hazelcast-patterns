# Map journal workflow

**Status: planned.** There is no runnable code yet.

A map update can be the input to the next processing stage. This example will follow an item through a small workflow driven by map journal events.

## What you will learn

- Which journal events a stage needs to process.
- Why payloads and stage status can live in separate maps.
- How enrichment and stage updates work without triggering an endless feedback loop.

## What you will be able to observe

You will be able to follow the output of each stage and see what happens when an event is replayed or a status update repeats.

## Planned setup

An embedded member started by tests. The intended setup uses OSS features; the exact dependency version is not confirmed yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
