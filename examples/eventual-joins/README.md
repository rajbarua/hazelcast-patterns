# Eventual joins

**Status: planned.** There is no runnable code yet.

An event may arrive before a lookup record becomes visible. This example will explore how asynchronous retries give that record time to arrive without waiting forever.

## What you will learn

- How a retry can wait without blocking processing threads.
- How the retry limit and delay affect waiting.
- What happens when the required record never arrives.

## What you will be able to observe

You will be able to compare a delayed record with a permanently missing one, and see what happens to pending work during shutdown.

## Planned setup

An embedded member started by tests. The intended setup uses OSS features; the exact dependency version is not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
