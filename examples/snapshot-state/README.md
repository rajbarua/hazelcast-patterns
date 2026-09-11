# Snapshot state

**Status: planned.** There is no runnable code yet.

A streaming job needs its processing state after a restart. This example will explore what is saved in a snapshot and how that state becomes usable again.

## What you will learn

- Why small, named state objects are easier to inspect.
- Where Compact serializers and their registration matter.
- Why every member needs access to the state classes it uses.

## What you will be able to observe

You will be able to follow processing after a snapshot restore and explore a class-visibility failure with members in separate JVMs.

## Planned setup

Members started by tests. The intended setup uses OSS features; the exact dependency version is not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
