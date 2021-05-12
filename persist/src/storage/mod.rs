//! WIP

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use abomonation::abomonated::Abomonated;
use abomonation::{decode, encode};
use abomonation_derive::Abomonation;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;

use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::{Batch, BatchReader, Batcher, Cursor};
use timely::dataflow::operators::ToStream;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::error::Error;
use crate::persister::{PersistableV1, PersistableV2, PersisterV1, PersisterV2};
use crate::trace::abom::AbomonatedBatch;
use crate::trace::PersistedTrace;
use crate::{PairSnapshot, Persistable, PersistedId, Snapshot};

// WIP: feature gate the following
pub mod file;
pub mod s3;
pub mod sqlite;

pub trait Blob {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error>;
}

pub trait Buffer {
    fn write_sync(
        &mut self,
        id: PersistedId,
        updates: &[((Vec<u8>, Vec<u8>), u64, i64)],
    ) -> Result<(), Error>;
    fn snapshot(&self, id: PersistedId, lower: Option<u64>) -> Result<Box<dyn Snapshot>, Error>;
}

#[derive(Abomonation, Clone)]
pub(crate) struct BatchMeta {
    // TODO: Add some debug_asserts to verify that this matches the frontier
    // represented by the batches.
    pub(crate) frontier: Option<u64>,
    pub(crate) batch_keys: Vec<String>,
}

pub(crate) struct BlobPersisterCore {
    pub(crate) batcher: <OrdValBatch<Vec<u8>, Vec<u8>, u64, i64, usize> as Batch<
        Vec<u8>,
        Vec<u8>,
        u64,
        i64,
    >>::Batcher,
    pub(crate) blob_meta: BatchMeta,
    pub(crate) blob: Box<dyn Blob>,
    pub(crate) buf: Box<dyn Buffer>,
    // TODO: LRU
    pub(crate) blob_cache: HashMap<String, AbomonatedBatch>,
}

impl BlobPersisterCore {
    pub(crate) fn get_blob(&mut self, key: &String) -> Result<Option<AbomonatedBatch>, Error> {
        if let Some(batch) = self.blob_cache.get(key) {
            return Ok(Some(batch.clone()));
        }
        match self.blob.get(key.as_bytes())? {
            None => Ok(None),
            Some(bytes) => {
                let batch = unsafe { Abomonated::new(bytes) }.expect("WIP");
                let batch = AbomonatedBatch(Arc::new(batch));
                self.blob_cache.insert(key.clone(), batch.clone());
                Ok(Some(batch))
            }
        }
    }
}

pub struct BlobPersister {
    // TODO: Don't keep this all under one mutex.
    core: Arc<Mutex<BlobPersisterCore>>,
}

impl BlobPersister {
    pub fn new(blob: Box<dyn Blob>, buf: Box<dyn Buffer>) -> Result<Self, Error> {
        let blob_meta = if let Some(mut buf) = blob.get("META".as_bytes())? {
            unsafe { decode::<BatchMeta>(&mut buf) }
                .expect("WIP")
                .0
                .clone()
        } else {
            BatchMeta {
                frontier: None,
                batch_keys: Vec::new(),
            }
        };
        let batcher = <OrdValBatch<Vec<u8>, Vec<u8>, u64, i64, usize> as Batch<
            Vec<u8>,
            Vec<u8>,
            u64,
            i64,
        >>::Batcher::new();
        let core = BlobPersisterCore {
            blob,
            buf,
            blob_meta,
            batcher,
            blob_cache: HashMap::new(),
        };
        Ok(BlobPersister {
            core: Arc::new(Mutex::new(core)),
        })
    }

    fn blob_snapshot(&self) -> Result<BlobSnapshot, Error> {
        let mut core = self.core.lock()?;
        let mut batches = Vec::new();
        // WIP unfortunate extra clone
        let batch_keys = core.blob_meta.batch_keys.clone();
        for key in batch_keys.iter() {
            batches.push(core.get_blob(key)?.expect("WIP"));
        }
        let snap = BlobSnapshot {
            frontier: core.blob_meta.frontier,
            batches,
        };
        Ok(snap)
    }
}

impl PersisterV1 for BlobPersister {
    type Persistable = BlobPersistable;

    fn create_or_load(&mut self, id: PersistedId) -> Result<Persistable<BlobPersistable>, Error> {
        let persistable = BlobPersistable {
            id,
            core: self.core.clone(),
        };
        let _snap = {
            let blob_snap = self.blob_snapshot()?;
            let core = self.core.lock()?;
            let wal_snap = core.buf.snapshot(id, blob_snap.frontier)?;
            Box::new(PairSnapshot {
                s1: Box::new(blob_snap) as Box<dyn Snapshot>,
                s2: wal_snap,
            })
        };
        let _meta = Box::new(BlobMeta {
            _id: id,
            _core: self.core.clone(),
        });
        Ok(Persistable { id, p: persistable })
    }

    fn destroy(&mut self, id: PersistedId) -> Result<(), Error> {
        todo!()
    }
}

impl PersisterV2 for BlobPersister {
    type Persistable = BlobPersistable;
}

pub struct BlobPersistable {
    id: PersistedId,
    core: Arc<Mutex<BlobPersisterCore>>,
}

impl PersistableV1 for BlobPersistable {
    fn write_sync(&mut self, updates: &[((Vec<u8>, Vec<u8>), u64, i64)]) -> Result<(), Error> {
        let mut core = self.core.lock()?;
        core.buf.write_sync(self.id, updates)?;
        // WIP well this is unfortunate
        let mut batcher_updates = updates
            .iter()
            .map(|((k, v), t, r)| ((k.clone(), v.clone()), *t, *r))
            .collect();
        core.batcher.push_batch(&mut batcher_updates);
        Ok(())
    }

    fn advance(&mut self, ts: u64) {
        let mut core = self.core.lock().expect("WIP");
        if core.batcher.frontier().less_than(&ts) {
            return;
        }
        let upper = Antichain::from_elem(ts);
        let batch = core.batcher.seal(upper);
        let mut encoded = Vec::new();
        unsafe { encode(&batch, &mut encoded).expect("WIP") };
        let batch_name = format!(
            "{}-{}-{}.batch",
            self.id.0,
            batch.lower().elements().first().unwrap_or(&0),
            ts
        );
        core.blob
            .as_mut()
            .set(batch_name.as_bytes(), encoded)
            .expect("WIP");

        core.blob_meta.frontier = Some(ts);
        core.blob_meta.batch_keys.push(batch_name);
        let mut encoded = Vec::new();
        unsafe { encode(&core.blob_meta, &mut encoded).expect("WIP") };
        core.blob
            .as_mut()
            .set("META".as_bytes(), encoded)
            .expect("WIP");
    }
}

impl PersistableV2 for BlobPersistable {
    fn arranged<G>(&self, mut scope: G) -> Result<Arranged<G, crate::trace::PersistedTrace>, Error>
    where
        G: Scope,
        G::Timestamp: Lattice,
    {
        let stream = {
            // WIP this needs to be an operator so it doesn't just snapshot
            let mut core = self.core.lock()?;
            // WIP unfortunate extra clone
            let batch_keys = core.blob_meta.batch_keys.clone();
            // WIP check this filter condition. also sanity check that the batches
            // line up with no holes
            let batches = batch_keys
                .iter()
                .map(|key| core.get_blob(key).expect("WIP").expect("WIP"))
                .collect::<Vec<_>>();
            batches.to_stream(&mut scope)
        };
        let trace = PersistedTrace::new(self.core.clone());
        Ok(Arranged { stream, trace })
    }
}

pub struct BlobMeta {
    _id: PersistedId,
    _core: Arc<Mutex<BlobPersisterCore>>,
}

struct BlobSnapshot {
    frontier: Option<u64>,
    batches: Vec<AbomonatedBatch>,
}

impl Snapshot for BlobSnapshot {
    fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool {
        let batch = match self.batches.pop() {
            Some(batch) => batch,
            None => return false,
        };
        let mut cursor = batch.cursor();
        cursor.rewind_keys(&batch);
        cursor.rewind_vals(&batch);
        while cursor.key_valid(&batch) {
            while cursor.val_valid(&batch) {
                let key = cursor.key(&batch);
                let val = cursor.val(&batch);
                cursor.map_times(&batch, |ts, r| {
                    buf.push(((key.clone(), val.clone()), ts.clone(), *r as i64));
                });
                cursor.step_val(&batch);
            }
            cursor.step_key(&batch);
        }
        true
    }
}

#[cfg(proc_macro)]
mod tests {
    use std::error::Error;

    use super::file::{self, FileBuffer};
    use super::s3::{self, S3Blob};
    use super::*;

    #[test]
    fn blob_persister() -> Result<(), Box<dyn Error>> {
        let blob = S3Blob::new(s3::Config {})?;
        let buf = FileBuffer::new(file::Config {})?;

        let updates = vec![
            (
                ("foo-k".as_bytes().to_vec(), "foo-v".as_bytes().to_vec()),
                1,
                1,
            ),
            (
                ("foo-k".as_bytes().to_vec(), "foo-v".as_bytes().to_vec()),
                3,
                -1,
            ),
            (
                ("bar-k".as_bytes().to_vec(), "bar-v".as_bytes().to_vec()),
                1,
                1,
            ),
        ];

        // Initial dataflow
        {
            let mut p = BlobPersister::new(
                Box::new(blob.clone()) as Box<dyn Blob>,
                Box::new(buf.clone()) as Box<dyn Buffer>,
            )?;

            let (PersistableStream(mut write, mut snap), mut meta) = p.create_or_load(1)?;
            {
                // Nothing has been written yet so snap should be empty.
                let mut buf = Vec::new();
                assert_eq!(snap.read(&mut buf), false);
                assert!(buf.is_empty());
            }
            write.write_sync(&updates)?;

            // Everything starts in the wal
            assert_eq!(blob.entries(), 0);
            // Move some data from the wal into the blob storage
            meta.0.advance(1);
            // The blob storage is no longer empty
            assert!(blob.entries() > 0);
        }

        // Restart dataflow with existing data
        {
            let mut p = BlobPersister::new(
                Box::new(blob.clone()) as Box<dyn Blob>,
                Box::new(buf.clone()) as Box<dyn Buffer>,
            )?;

            let (PersistableStream(_write, mut snap), _meta) = p.create_or_load(1)?;
            {
                // Verify that the snap contains the data we wrote before the restart.
                let mut snap_contents = Vec::new();
                while snap.read(&mut snap_contents) {}
                snap_contents.sort();
                let mut expected = updates.clone();
                expected.sort();
                assert_eq!(snap_contents, expected);
            }
        }

        Ok(())
    }
}
