//! WIP

use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::persister::{PersistableV1, PersisterV1};
use crate::{Persistable, PersistedId};

// TODO: Real SQLite impl.

pub struct Config {}

pub struct SQLiteManager {
    dataz: HashMap<PersistedId, Arc<Mutex<Vec<((Vec<u8>, Vec<u8>), u64, i64)>>>>,
}

impl SQLiteManager {
    pub fn new(_c: Config) -> Result<Self, Error> {
        Ok(SQLiteManager {
            dataz: HashMap::new(),
        })
    }
}

impl PersisterV1 for SQLiteManager {
    type Persistable = SQLite;

    fn create_or_load(&mut self, id: PersistedId) -> Result<Persistable<SQLite>, Error> {
        let dataz = match self.dataz.entry(id) {
            hash_map::Entry::Occupied(_) => {
                return Err(Error::String(format!("id already registered: {}", id.0)))
            }
            hash_map::Entry::Vacant(e) => e.insert(Default::default()),
        };
        let persistable = SQLite {
            dataz: dataz.clone(),
        };
        let _snapshot = Box::new(SQLiteSnapshot {
            dataz: dataz.lock()?.clone(),
        });
        let _meta = Box::new(SQLite {
            dataz: dataz.clone(),
        });
        Ok(Persistable { id, p: persistable })
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
    dataz: Arc<Mutex<Vec<((Vec<u8>, Vec<u8>), u64, i64)>>>,
}

impl PersistableV1 for SQLite {
    fn write_sync(&mut self, updates: &[((Vec<u8>, Vec<u8>), u64, i64)]) -> Result<(), Error> {
        self.dataz.lock().expect("WIP").extend_from_slice(updates);
        Ok(())
    }
    fn advance(&mut self, ts: u64) {
        todo!()
    }
}

pub struct SQLiteSnapshot {
    dataz: Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
}

// impl PersistedStreamSnapshot for SQLiteSnapshot {
//     fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool {
//         buf.append(&mut self.dataz);
//         return false;
//     }
// }
