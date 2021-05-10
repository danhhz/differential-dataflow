//! WIP

use std::error::Error;
use std::sync::{Arc, Mutex};

use abomonation;
use abomonation_derive::Abomonation;

use crate::storage::Wal;
use crate::PersistedStreamSnapshot;

pub struct Config {}

#[derive(Clone)]
pub struct FileWal {
    dataz: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FileWal {
    pub fn new(_c: Config) -> Result<Self, Box<dyn Error>> {
        Ok(FileWal {
            dataz: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

#[derive(Abomonation)]
struct WalEntry {
    id: u64,
    updates: Vec<(Vec<u8>, u64, i64)>,
}

impl Wal for FileWal {
    fn write_sync(
        &mut self,
        id: u64,
        updates: &[(Vec<u8>, u64, i64)],
    ) -> Result<(), Box<dyn Error>> {
        let entry = WalEntry {
            id: id,
            updates: updates.to_vec(),
        };
        let mut buf = Vec::new();
        unsafe {
            abomonation::encode(&entry, &mut buf)?;
        }
        self.dataz.lock().expect("WIP").push(buf);
        Ok(())
    }

    fn snapshot(
        &self,
        id: u64,
        lower: Option<u64>,
    ) -> Result<Box<dyn PersistedStreamSnapshot>, Box<dyn Error>> {
        let s = FileWalSnapshot {
            id,
            lower,
            // WIP lol this is terrible
            dataz: self.dataz.lock().expect("WIP").clone(),
        };
        Ok(Box::new(s) as Box<dyn PersistedStreamSnapshot>)
    }
}

pub struct FileWalSnapshot {
    id: u64,
    lower: Option<u64>,
    dataz: Vec<Vec<u8>>,
}

impl PersistedStreamSnapshot for FileWalSnapshot {
    fn read(&mut self, buf: &mut Vec<(Vec<u8>, u64, i64)>) -> bool {
        while let Some(mut dataz) = self.dataz.pop() {
            let entry = unsafe { abomonation::decode::<WalEntry>(&mut dataz) };
            let (entry, _) = entry.expect("WIP");
            if self.id != entry.id {
                continue;
            }
            buf.extend(
                entry
                    .updates
                    .iter()
                    .filter(|(_, ts, _)| Some(*ts) >= self.lower)
                    .cloned(),
            );
            if buf.is_empty() {
                continue;
            }
            return true;
        }
        return false;
    }
}
