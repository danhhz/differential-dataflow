# Persist

This is an in-progress prototype for a general persistence abstraction for
Materialize.

### Overview

NB: There's a couple V1/V2 traits present with the V2 extending the
corresponding V1. This is not a representation of how it would look in
production, but rather proving how we could incrementally build persistence. V1
is sufficient for table persistence. V2 is necessary for operator persistence.
Source persistence could possibly be built on V1, but it's probably better to
wait for V2 so we can use it for timestamping and de-upserting.

A persistence user would start by constructing a `PersistManager` which is a
thin `Send+Sync+Clone` handle around a `PersisterV1/V2` impl which does the
heavy lifting (see details below). `PersistManager` is a registry for multiple
`Persisted`s (each corresponding to a table, source, or operator) in a single
process. This allows us to funnel everything a process is persisting through a
single place for batching and rate limiting.

Given a unique identifier, `PersistManager` will hand back a `Persisted` token.
This can be handed in (consumed) to create a timely operator that persists and
passes through its input. Concretely:

```rust
let persisted = manager.create_or_load(id)?;
let persisted_stream = stream.persist_unary_sync(persisted);
let meta = persisted_stream.meta().clone();
persisted_stream.into_stream()
```

The return value from `persist_unary_sync` is a `PersistedStream`, which can
hand back a `PersistedMeta` metadata handle (used for probing
progress/unblocking compaction in V1 or accessing the arrangement in V2). This
handle can also be consumed to return a timely stream with the input data (after
it's been persisted for the `*_sync` variants). There is a parallel
`persist_collection_sync` and `PersistedCollection`, which does the same for a
differential collection.

- TODO: This method of getting the metadata handle ends up being pretty clunky
  in practice. Maybe instead the user should pass in a mutable reference to a
  `PersistedMeta` they've constructed like `probe_with`?

I originally wrote `persist_collection_sync` hoping that it would be nicely
composable, but that turns out to not be the case. It's probably closest to how
we'd do source persistence. However, as Ruchir pointed out, for table
persistence, we'd want to block acknowledging an `INSERT` until the data has
been persisted (which passing an input source into `persist_unary_sync` doesn't
do). We could probe the PersistentStream, but that adds unnecessary latency.
Instead, we probably want a variant that returns an `InputHandle` or
`InputSession` and internally makes sure the data has been persisted before
`send` returns. Operator persistence presumably wants yet another variant that
wraps an operator as the thesis does.

### V1 and V2

`PersisterV1` basically just supports synchronously writing data out, grabbing a
snapshot of everything written so far, and unblocking compaction of everything
less than a given timestamp. It could implemented on top of a SQLite table with
something like the following schema:

```sql
CREATE TABLE persisted (persisted_id INT, key BYTES, val BYTES, ts BYTES, diff BYTES);
CREATE INDEX ON persisted (persisted_id, ts, key);
```

- TODO: At the moment, the interface supports grabbing a snapshot at any point,
  but the only time it's used is when everything is first loading up and before
  any writes have happened. My intuition is that the current situation is overly
  general and will lead to unnecessary complexity, but my initial attempts to
  give back a snapshot only on load have been clunky and unsatisfying.

`PersistedV2` notably adds support for accessing the persisted data as an
arrangement. This would be useful in source persistence because it could
performantly answer "have we assigned this data a timestamp before?" (for
timestamp persistence) and "what was the previous value for this key?" (for
de-upserting). The arrangement is also a key piece of Lorenzo's thesis on
operator persistence. (Still missing is Lorenzo's FutureLog, which could be
added to `PersistedV2` but I haven't gotten there yet.)

`PersistedV2` has one implementation, which is itself in terms of the `Buffer`
and `Blob` traits. This would be the only implementation in the productionized
version, so it's really only a trait here for the sake of the V1/V2
incrementally buildable exploration. Internally, any data is immediately handed
to the Buffer, which could be durable for source persistence (a WAL on EBS or
maybe redpanda) or not durable for operator persistence. As timestamps are
closed, data from the buffer is moved into a persistent trace build on top of
`Blob` which is a KV abstraction over S3 and files. For operator persistence,
we'll have to add a FutureLog holding pen which immediately slurps everything
from the Buffer (or they're tee'd?) and indexes it in the necessary way.

- Unpolished thought: My initial thought for migrating data from the buffer into
  the futurelog/blob is a separate thread, but the step model is nicely
  deterministic. Maybe the buffer/futurelog/persisted trace could themselves be
  a dataflow which gets scheduled by the worker like any other? This would have
  the added benefit of letting us hook into any tooling we build for
  monitoring/debugging dataflows. (And might have some nice scheduling
  properties?)

### Seems good
- Uses a single timely operator over a stream both for receiving the data to
  persist as well as for rehydrating it on dataflow restart.
- I feel pretty good about the storage module's organization and the Blob/Wal
  split.
- The performance of a columnar capnp example is within 2x of OrdValBatch, which
  seems good enough

### Needs work
- The async story. I haven't grokked how async plays with timely yet, so dunno
  if/where that fits in yet.
- Error handling. Right now, there are a bunch of `expect`s and this likely
  needs to hook into the error streams that Materialize hands around.
- Abomonation was used as a serialization format to enable fast iteration and
  that has worked splendidly, it's stupidly easy to use and has no boilerplate.
  I'd be more comfortable using an encoding with a defined spec for the real
  impl of this though.
- The locking granularity in BlobPersister is too coarse grained.
- There's placeholder logic for how BlobPersister keeps metadata on the batches,
  but it hasn't been thought through.
- I'd love some testing where we forcably kill processes in the middle of
  persisting things and verify invariants on dataflow restart. Something vaguely
  like this: https://gist.github.com/danhhz/264b7219b0bc64156ff1c829f3e1ba9c
- Related to the above, I've found jepsen-style "nemesis" based testing useful
  in the past, though it's unclear what nemeses we have besides process
  interruption.
- Compactions are still unimplemented.
- Blobs (think S3) should likely be cached in some sort of LRU. Unclear where
  this lives. I originally was thinking inside the S3Blob impl, but maybe we
  want it to be after decode? Dunno.
- What's our story with poisoned mutexes?
- Where do the various pieces live (mz/dd/new crate)?
- Clean up the `pub(crate)`s.
- A `String` key and `String` value are currently hardcoded, but this needs to
  be generalized.
- TODO: These were my immediate thoughts but there's stuff I'm forgetting. Flesh
  this list out.
