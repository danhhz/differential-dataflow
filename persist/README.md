# Persist

This is an in-progress prototype for a general persistence abstraction for
Materialize.

TODO overview

### Seems good
- Uses a single timely operator over a stream both for receiving the data to
  persist as well as for rehydrating it on dataflow restart.
- I feel pretty good about the storage module's organization and the Blob/Wal
  split.
- The performance of a columnar capnp example is within 2x of OrdValBatch, which
  seems good enough

### Needs work
- The whole vision of this is that we can have it implement Arranged, which is a
  crazy useful building block for upsert (mz source persistence), join/reduce
  (operator persistence), etc but that's still a TODO. This likely requires some
  small tweaks to the existing trace traits so that we can return a `&[u8]`
  instead of a `&Vec<u8>`.
- `Box<dyn Foo>` everywhere. This was introduced as an attempt to keep prototype
  iteration fast, but it's spread everywhere (and it's not even clear that it
  does help with fast iteration).
- The async story. I haven't grokked how async plays with timely yet, so dunno
  if/where that fits in yet.
- Error handling. Right now, it's all `Result`s with `Box<dyn Error>`. These
  errors need to be structured and they likely need to hook into the error
  streams that Materialize hands around.
- Persister/PersistedStream*/PersistableStream/PersistableMeta. The public
  interface for this crate is messy and confusingly named.
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
