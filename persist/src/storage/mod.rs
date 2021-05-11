//! WIP

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use abomonation::abomonated::Abomonated;
use abomonation::{decode, encode};
use abomonation_derive::Abomonation;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::implementations::ord::OrdKeyBatch;
use differential_dataflow::trace::{Batch, BatchReader, Batcher, Cursor};
use timely::dataflow::operators::ToStream;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::abom::AbomonatedBatch;
use crate::{
    PersistableMeta, PersistableStream, PersistedStreamMeta, PersistedStreamSnapshot,
    PersistedStreamWrite, PersistedTraceReader, Persister,
};

// WIP: feature gate the following
pub mod file;
pub mod s3;
pub mod sqlite;

pub trait Blob {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>>;
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Box<dyn Error>>;
}

pub trait Buffer {
    fn write_sync(
        &mut self,
        id: u64,
        updates: &[(Vec<u8>, u64, i64)],
    ) -> Result<(), Box<dyn Error>>;
    fn snapshot(
        &self,
        id: u64,
        lower: Option<u64>,
    ) -> Result<Box<dyn PersistedStreamSnapshot>, Box<dyn Error>>;
}

#[derive(Abomonation, Clone)]
struct BatchMeta {
    // TODO: Add some debug_asserts to verify that this matches the frontier
    // represented by the batches.
    frontier: Option<u64>,
    batch_keys: Vec<String>,
}

struct BlobPersisterCore {
    batcher: <OrdKeyBatch<Vec<u8>, u64, i64, usize> as Batch<Vec<u8>, (), u64, i64>>::Batcher,
    blob_meta: BatchMeta,
    blob: Box<dyn Blob>,
    buf: Box<dyn Buffer>,
    // TODO: LRU
    blob_cache: HashMap<String, AbomonatedBatch>,
}

impl BlobPersisterCore {
    fn get_blob(&mut self, key: &String) -> Result<Option<AbomonatedBatch>, Box<dyn Error>> {
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
    pub fn new(blob: Box<dyn Blob>, buf: Box<dyn Buffer>) -> Result<Self, Box<dyn Error>> {
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
        let batcher =
            <OrdKeyBatch<Vec<u8>, u64, i64, usize> as Batch<Vec<u8>, (), u64, i64>>::Batcher::new();
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

    fn blob_snapshot(&self) -> Result<BlobSnapshot, Box<dyn Error>> {
        let mut core = self.core.lock().expect("WIP");
        let mut batches = Vec::new();
        // WIP unfortunate extra clone
        let batch_keys = core.blob_meta.batch_keys.clone();
        for key in batch_keys.iter() {
            batches.push(core.get_blob(key).expect("WIP").expect("WIP"));
        }
        let snap = BlobSnapshot {
            frontier: core.blob_meta.frontier,
            batches,
        };
        Ok(snap)
    }
}

impl Persister for BlobPersister {
    fn create_or_load(
        &mut self,
        id: u64,
    ) -> Result<(PersistableStream, PersistableMeta), Box<dyn Error>> {
        let write = Box::new(BlobWrite {
            id,
            core: self.core.clone(),
        }) as Box<dyn PersistedStreamWrite>;
        let snap = {
            let blob_snap = self.blob_snapshot()?;
            let core = self.core.lock().expect("WIP");
            let wal_snap = core.buf.snapshot(id, blob_snap.frontier)?;
            Box::new(PairSnapshot {
                s1: Box::new(blob_snap) as Box<dyn PersistedStreamSnapshot>,
                s2: wal_snap,
            }) as Box<dyn PersistedStreamSnapshot>
        };
        let meta = Box::new(BlobMeta {
            id,
            core: self.core.clone(),
        });
        Ok((PersistableStream(write, snap), PersistableMeta(meta)))
    }

    fn arranged<G>(&self, mut scope: G, _id: u64) -> Arranged<G, crate::PersistedTraceReader>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord,
    {
        let mut core = self.core.lock().expect("WIP");
        // WIP
        let stream = None.to_stream(&mut scope);
        // WIP unfortunate extra clone
        let batch_keys = core.blob_meta.batch_keys.clone();
        let batches = batch_keys
            .iter()
            .map(|key| core.get_blob(key).expect("WIP").expect("WIP"))
            .collect();
        let trace = PersistedTraceReader::new(batches);
        Arranged { stream, trace }
    }
}

pub struct BlobWrite {
    id: u64,
    core: Arc<Mutex<BlobPersisterCore>>,
}

impl PersistedStreamWrite for BlobWrite {
    fn write_sync(&mut self, updates: &[(Vec<u8>, u64, i64)]) -> Result<(), Box<dyn Error>> {
        let mut core = self.core.lock().expect("WIP");
        core.buf.write_sync(self.id, updates)?;
        // WIP well this is unfortunate
        let mut batcher_updates = updates
            .iter()
            .map(|(k, t, r)| ((k.clone(), ()), *t, *r))
            .collect();
        core.batcher.push_batch(&mut batcher_updates);
        Ok(())
    }
}

pub struct BlobMeta {
    id: u64,
    core: Arc<Mutex<BlobPersisterCore>>,
}

impl PersistedStreamMeta for BlobMeta {
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
            self.id,
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

    fn allow_compaction(&mut self, _ts: u64) {
        todo!()
    }

    fn destroy(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

struct BlobSnapshot {
    frontier: Option<u64>,
    batches: Vec<AbomonatedBatch>,
}

impl PersistedStreamSnapshot for BlobSnapshot {
    fn read(&mut self, buf: &mut Vec<(Vec<u8>, u64, i64)>) -> bool {
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
                cursor.map_times(&batch, |ts, r| {
                    buf.push((key.clone(), ts.clone(), r.clone()));
                });
                cursor.step_val(&batch);
            }
            cursor.step_key(&batch);
        }
        true
    }
}

struct PairSnapshot {
    s1: Box<dyn PersistedStreamSnapshot>,
    s2: Box<dyn PersistedStreamSnapshot>,
}

impl PersistedStreamSnapshot for PairSnapshot {
    fn read(&mut self, buf: &mut Vec<(Vec<u8>, u64, i64)>) -> bool {
        self.s1.read(buf) || self.s2.read(buf)
    }
}

#[cfg(test)]
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
            ("foo".as_bytes().to_vec(), 1, 1),
            ("foo".as_bytes().to_vec(), 3, -1),
            ("bar".as_bytes().to_vec(), 2, 1),
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
