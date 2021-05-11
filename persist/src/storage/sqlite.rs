//! WIP

use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};

use crate::{
    PersistableMeta, PersistableStream, PersistedStreamMeta, PersistedStreamSnapshot,
    PersistedStreamWrite, Persister,
};

// TODO: Real SQLite impl.

pub struct Config {}

pub struct SQLiteManager {
    dataz: HashMap<u64, Arc<Mutex<Vec<((Vec<u8>, Vec<u8>), u64, i64)>>>>,
}

impl SQLiteManager {
    pub fn new(_c: Config) -> Result<Self, Box<dyn Error>> {
        Ok(SQLiteManager {
            dataz: HashMap::new(),
        })
    }
}

impl Persister for SQLiteManager {
    fn create_or_load(
        &mut self,
        id: u64,
    ) -> Result<(PersistableStream, PersistableMeta), Box<dyn Error>> {
        let dataz = self.dataz.entry(id).or_default();
        let writer = Box::new(SQLite {
            dataz: dataz.clone(),
        });
        let snapshot = Box::new(SQLiteSnapshot {
            dataz: dataz.lock().expect("WIP").clone(),
        });
        let meta = Box::new(SQLite {
            dataz: dataz.clone(),
        });
        Ok((PersistableStream(writer, snapshot), PersistableMeta(meta)))
    }

    fn arranged<G>(
        &self,
        _scope: G,
        _id: u64,
    ) -> differential_dataflow::operators::arrange::Arranged<G, crate::PersistedTraceReader>
    where
        G: timely::dataflow::Scope,
        G::Timestamp: differential_dataflow::lattice::Lattice + Ord,
    {
        todo!()
    }
}

#[derive(Clone)]
pub struct SQLite {
    dataz: Arc<Mutex<Vec<((Vec<u8>, Vec<u8>), u64, i64)>>>,
}

impl PersistedStreamWrite for SQLite {
    fn write_sync(
        &mut self,
        updates: &[((Vec<u8>, Vec<u8>), u64, i64)],
    ) -> Result<(), Box<dyn Error>> {
        self.dataz.lock().expect("WIP").extend_from_slice(updates);
        Ok(())
    }
}

impl PersistedStreamMeta for SQLite {
    fn advance(&mut self, _ts: u64) {
        todo!()
    }

    fn allow_compaction(&mut self, _ts: u64) {
        todo!()
    }

    fn destroy(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

pub struct SQLiteSnapshot {
    dataz: Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
}

impl PersistedStreamSnapshot for SQLiteSnapshot {
    fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool {
        buf.append(&mut self.dataz);
        return false;
    }
}
