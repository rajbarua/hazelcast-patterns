# Inter-job backpressure

**Status: planned.** There is no runnable code yet.

One job can produce work faster than another job can consume it. This example will explore how to slow incoming work when a backlog grows between jobs.

## What you will learn

- How backlog between jobs differs from backpressure inside one job.
- How a feedback signal avoids repeatedly switching between faster and slower rates.
- How a token bucket controls the incoming rate and allows limited bursts.

## What you will be able to observe

You will be able to see how ingress changes as a downstream job slows down, recovers, or stops providing a fresh control signal.

## Planned setup

An embedded member started by tests. The intended setup uses OSS features; the exact dependency version is not confirmed yet.

You will be able to run this example on its own, without building or starting the other projects. Run commands are not available yet.

[All examples](../../README.md#examples) · [Learning paths](../../docs/learning-paths.md)
