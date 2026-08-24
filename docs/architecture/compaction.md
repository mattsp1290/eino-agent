# Compaction Boundaries

Compaction is represented as a context epoch transition plus a replayable
summary boundary.

The first milestone keeps the behavior explicit:

- every admitted run starts a durable `session.ContextEpoch`;
- stores can expose `session.ContextEpochReader` to list context epochs for a
  session so audit/replay code can explain which epoch was active;
- a compaction summary is a system `session.Message` with a `PartCompaction`
  payload;
- the payload stores summary text and message IDs for the summarized range, not
  omitted raw prompt content;
- history projection includes the summary message and retained tail when an
  epoch is supplied.

`session/compaction.AppendBoundary` writes the summary message and part, then
finishes the epoch with the summary message ID. It uses `session.Store.WithinTx`
when the supplied store implements it; otherwise callers should treat the
operation as sequential store writes.

This gives embedders a small durable boundary they can build on without adding
automatic summarization or token-budget policy in the runtime yet.
