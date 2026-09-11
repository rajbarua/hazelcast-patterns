# Contributing

Keep examples small enough to read and run on their own.

## Before adding an example

Choose one main question the example will answer. Related patterns can share the same project if they help answer that question. Add a separate example when combining them would make the code harder to follow.

Use a small, synthetic data model. Do not copy private deployment settings, internal links, real customer data or credentials.

## Example structure

When an example is implemented, use this structure:

```text
example-name/
  README.md
  pom.xml
  src/main/java/
  src/test/java/
  diagrams/
```

This is a suggested Java layout, not a root Maven build. Add scripts or service configuration only when the example needs them. Do not create empty source files or pretend build commands work before there is runnable code.

Each example owns its build and dependencies. It must not depend on sibling examples. A composition also runs independently; it can repeat a small amount of code and link to the simpler explanation.

## README contents

Explain:

1. The problem and the main idea.
2. What the diagram shows.
3. Which code to read first.
4. Requirements, including Hazelcast version and edition.
5. Exact commands to run the example and its tests.
6. Expected results and how failures are tested.
7. What is guaranteed, what is not, and related examples.

Use plain language. Define terms such as partition, journal or snapshot when they first matter.

Write READMEs and learning docs for learners. Describe what they will learn, run and observe. Keep implementation tasks, writing rules and maintenance instructions in this file or AGENTS.md. Planned examples should describe the learning topic without reading like a task list for their author.

## Tests

Start with a test of the main result. Add a failure or boundary case that makes the pattern worth learning. For example, replay an item, delay a lookup or stop a member.

Tests should own startup and teardown. Use bounded waits with clear failure messages. Assert actual results and counts; checking that every item matches a condition is not enough if the result can be empty.

Use separate JVMs when process failure or member classpaths are part of the lesson. Use timing measurements to explore performance, not as the only proof that work ran locally.

## Diagrams

Put diagrams in the example's `diagrams/` folder, beside its README and source code. Keep editable sources and exported SVG or PNG images together, and embed images in the README so readers do not need a diagram editor.

Use draw.io or Excalidraw when an editable canvas helps. Small Mermaid diagrams can live directly in Markdown.

Choose diagrams that explain data ownership, data flow, failure and recovery, or the sequence of calls. Use the same names as the code. Label network boundaries and distinguish local work from remote calls. Keep diagrams small; use a separate diagram for a failure scenario when it would obscure the main flow.

## Repository changes

Update the root README and pattern index when an example becomes runnable. Keep planned work clearly marked.
