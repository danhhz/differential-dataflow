//! WIP

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::storage::Blob;

// TODO: Real s3 impl.

pub struct Config {}

#[derive(Clone)]
pub struct S3Blob {
    dataz: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl S3Blob {
    pub fn new(_c: Config) -> Result<Self, Error> {
        Ok(S3Blob {
            dataz: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn entries(&self) -> usize {
        self.dataz.lock().expect("WIP").len()
    }
}

impl Blob for S3Blob {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        if let Some(val) = self.dataz.lock().expect("WIP").get(key) {
            Ok(Some(val.clone()))
        } else {
            Ok(None)
        }
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        // WIP param to err on overwrite?
        self.dataz.lock().expect("WIP").insert(key.to_vec(), value);
        Ok(())
    }
}
