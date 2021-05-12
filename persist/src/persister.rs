//! WIP

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use timely::dataflow::Scope;

use crate::error::Error;
use crate::{Persistable, PersistedId};

pub trait PersistableV1 {
    fn write_sync(&mut self, updates: &[((Vec<u8>, Vec<u8>), u64, i64)]) -> Result<(), Error>;
    fn advance(&mut self, ts: u64);
}

pub trait PersistableV2 {
    fn arranged<G>(&self, scope: G) -> Result<Arranged<G, crate::trace::PersistedTrace>, Error>
    where
        G: Scope,
        G::Timestamp: Lattice;
}

pub trait PersisterV1: Sized {
    type Persistable: PersistableV1;

    fn create_or_load(&mut self, id: PersistedId) -> Result<Persistable<Self::Persistable>, Error>;
    fn destroy(&mut self, id: PersistedId) -> Result<(), Error>;
}

pub trait PersisterV2: PersisterV1 {
    type Persistable: PersistableV2;
}
