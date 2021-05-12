//! WIP

use std::sync::{Arc, Mutex};

use abomonation;
use abomonation_derive::Abomonation;

use crate::error::Error;
use crate::storage::Buffer;
use crate::{PersistedId, Snapshot};

pub struct Config {}

#[derive(Clone)]
pub struct FileBuffer {
    dataz: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FileBuffer {
    pub fn new(_c: Config) -> Result<Self, Error> {
        Ok(FileBuffer {
            dataz: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

#[derive(Abomonation)]
struct WalEntry {
    id: u64,
    updates: Vec<((Vec<u8>, Vec<u8>), u64, i64)>,
}

impl Buffer for FileBuffer {
    fn write_sync(
        &mut self,
        id: PersistedId,
        updates: &[((Vec<u8>, Vec<u8>), u64, i64)],
    ) -> Result<(), Error> {
        let entry = WalEntry {
            id: id.0,
            updates: updates.to_vec(),
        };
        let mut buf = Vec::new();
        unsafe {
            abomonation::encode(&entry, &mut buf)?;
        }
        self.dataz.lock()?.push(buf);
        Ok(())
    }

    fn snapshot(&self, id: PersistedId, lower: Option<u64>) -> Result<Box<dyn Snapshot>, Error> {
        let s = FileWalSnapshot {
            id: id.0,
            lower,
            // WIP lol this is terrible
            dataz: self.dataz.lock()?.clone(),
        };
        Ok(Box::new(s) as Box<dyn Snapshot>)
    }
}

pub struct FileWalSnapshot {
    id: u64,
    lower: Option<u64>,
    dataz: Vec<Vec<u8>>,
}

impl Snapshot for FileWalSnapshot {
    fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool {
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
