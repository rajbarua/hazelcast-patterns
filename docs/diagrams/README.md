# Reading the diagrams

The examples include diagrams alongside the code. Start with the [local-compute sequence and ownership diagrams](../../examples/compute-with-data/README.md) to compare client-side updates with owner-side computation.

Different views will help you answer different questions:

| View | What to look for |
| --- | --- |
| Data ownership | Which member owns a record, and where does the code run? |
| Data flow | Where does an event enter, which stages process it, and where does the result go? |
| Failure and recovery | What stops working, what state survives, and which work runs again? |
| Sequence | In what order do calls, retries and commits happen? |

To explore a variation, edit the Mermaid source in the example README or the source files in its `diagrams/` folder.

[Learning paths](../learning-paths.md) · [Pattern index](../pattern-index.md)
