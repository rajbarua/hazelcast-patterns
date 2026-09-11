# Journal recovery

**Status: planned.** There is no runnable code yet.

A slow consumer can miss events when a journal fills up. This example will explore what can be recovered from the state still held in the map.

## What you will learn

- How a consumer detects lost journal events.
- How retained map entries help rebuild working state.
- Why live and recovered records can overlap.
- Why current map values cannot recreate every historical update.

## What you will be able to observe

You will be able to see a small journal overflow and follow recovery without counting the same retained record twice.

## Planned setup

Members started by tests. The required Hazelcast version is not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
