//! WIP

use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::persister::{MetaV1, PersisterV1, Snapshot, WriteV1};
use crate::{Persistable, PersistedId};

// TODO: Real SQLite impl.

pub struct SQLiteManager {
    dataz: HashMap<PersistedId, Arc<Mutex<Vec<((String, String), u64, isize)>>>>,
}

impl SQLiteManager {
    pub fn new() -> Self {
        SQLiteManager {
            dataz: HashMap::new(),
        }
    }
}

impl PersisterV1 for SQLiteManager {
    type Write = SQLite;
    type Meta = SQLite;

    fn create_or_load(
        &mut self,
        id: PersistedId,
    ) -> Result<Persistable<Self::Write, Self::Meta>, Error> {
        let dataz = match self.dataz.entry(id) {
            hash_map::Entry::Occupied(_) => {
                return Err(Error::String(format!("id already registered: {}", id.0)))
            }
            hash_map::Entry::Vacant(e) => e.insert(Default::default()),
        };
        let p = SQLite {
            dataz: dataz.clone(),
        };
        Ok(Persistable {
            id,
            write: p.clone(),
            meta: p,
        })
    }

    fn destroy(&mut self, id: PersistedId) -> Result<(), Error> {
        if let Some(_) = self.dataz.remove_entry(&id) {
            Ok(())
        } else {
            Err(Error::String(format!(
                "could not destroy unregistered id: {}",
                id.0
            )))
        }
    }
}

#[derive(Clone)]
pub struct SQLite {
    dataz: Arc<Mutex<Vec<((String, String), u64, isize)>>>,
}

impl WriteV1 for SQLite {
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error> {
        self.dataz.lock().expect("WIP").extend_from_slice(updates);
        Ok(())
    }
}

impl MetaV1 for SQLite {
    type Snapshot = SQLiteSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, Error> {
        let dataz = self.dataz.lock()?.clone();
        let snap = SQLiteSnapshot { dataz };
        Ok(snap)
    }

    fn advance(&mut self, ts: u64) {
        let mut dataz = self.dataz.lock().expect("WIP");
        dataz.retain(|(_, t, _)| *t > ts);
    }

    fn allow_compaction(&mut self, ts: u64) {
        todo!()
    }
}

pub struct SQLiteSnapshot {
    dataz: Vec<((String, String), u64, isize)>,
}

impl Snapshot for SQLiteSnapshot {
    fn read(&mut self, buf: &mut Vec<((String, String), u64, isize)>) -> bool {
        buf.append(&mut self.dataz);
        return false;
    }
}
