# Reading the diagrams

The examples will include diagrams alongside the code. There are no diagrams yet.

Different views will help you answer different questions:

| View | What to look for |
| --- | --- |
| Data ownership | Which member owns a record, and where does the code run? |
| Data flow | Where does an event enter, which stages process it, and where does the result go? |
| Failure and recovery | What stops working, what state survives, and which work runs again? |
| Sequence | In what order do calls, retries and commits happen? |

For local compute, pay attention to the member and partition boundaries. A step that runs beside its data may avoid a remote fetch, while another step may send data to a different member.

The diagrams will use the same names as the code so you can follow a record from one to the other. Images will appear in the example READMEs. Editable sources will also be available in each example's `diagrams/` folder if you want to explore a variation.

[Learning paths](../learning-paths.md) · [Pattern index](../pattern-index.md)
