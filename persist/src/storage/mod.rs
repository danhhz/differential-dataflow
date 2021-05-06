//! WIP

use std::error::Error;
use std::sync::{Arc, Mutex};

use abomonation::{decode, encode};
use abomonation_derive::Abomonation;
use differential_dataflow::trace::implementations::ord::OrdKeyBatch;
use differential_dataflow::trace::{Batch, BatchReader, Batcher};
use timely::progress::Antichain;

use crate::{
    PersistableMeta, PersistableStream, PersistedStreamMeta, PersistedStreamSnapshot,
    PersistedStreamWrite, Persister,
};

// WIP: feature gate the following
pub mod file;
pub mod s3;
pub mod sqlite;

pub trait Blob {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn Error>>;
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Box<dyn Error>>;
}

pub trait Wal {
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
    wal: Box<dyn Wal>,
}

pub struct BlobPersister {
    // TODO: Don't keep this all under one mutex.
    core: Arc<Mutex<BlobPersisterCore>>,
}

impl BlobPersister {
    pub fn new(blob: Box<dyn Blob>, wal: Box<dyn Wal>) -> Result<Self, Box<dyn Error>> {
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
            wal,
            blob_meta,
            batcher,
        };
        Ok(BlobPersister {
            core: Arc::new(Mutex::new(core)),
        })
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
            let core = self.core.lock().expect("WIP");
            let blob_frontier = core.blob_meta.frontier;
            // WIP also play back the values in blob
            core.wal.snapshot(id, blob_frontier)
        }?;
        let meta = Box::new(BlobMeta {
            id,
            core: self.core.clone(),
        });
        Ok((PersistableStream(write, snap), PersistableMeta(meta)))
    }
}

pub struct BlobWrite {
    id: u64,
    core: Arc<Mutex<BlobPersisterCore>>,
}

impl PersistedStreamWrite for BlobWrite {
    fn write_sync(&mut self, updates: &[(Vec<u8>, u64, i64)]) -> Result<(), Box<dyn Error>> {
        self.core
            .lock()
            .expect("WIP")
            .wal
            .write_sync(self.id, updates)
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::file::{self, FileWal};
    use super::s3::{self, S3Blob};
    use super::*;

    #[test]
    fn blob_persister() -> Result<(), Box<dyn Error>> {
        let blob = S3Blob::new(s3::Config {})?;
        let wal = FileWal::new(file::Config {})?;

        let updates = vec![
            ("foo".as_bytes().to_vec(), 1, 1),
            ("foo".as_bytes().to_vec(), 3, -1),
            ("bar".as_bytes().to_vec(), 2, 1),
        ];

        // Initial start
        {
            let mut p = BlobPersister::new(
                Box::new(blob.clone()) as Box<dyn Blob>,
                Box::new(wal.clone()) as Box<dyn Wal>,
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

        // Restart with existing data
        {
            let mut p = BlobPersister::new(
                Box::new(blob.clone()) as Box<dyn Blob>,
                Box::new(wal.clone()) as Box<dyn Wal>,
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
