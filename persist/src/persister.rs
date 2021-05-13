//! WIP

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use timely::dataflow::Scope;

use crate::error::Error;
use crate::{Persistable, PersistedId};

pub trait WriteV1 {
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error>;
}

pub trait MetaV1: Clone {
    type Snapshot: Snapshot;
    fn snapshot(&self) -> Result<Self::Snapshot, Error>;
    fn allow_compaction(&mut self, ts: u64);
}

pub trait WriteV2: WriteV1 {}

pub trait MetaV2: MetaV1 {
    fn advance(&mut self, ts: u64);
    fn arranged<G>(&self, scope: G) -> Result<Arranged<G, crate::trace::PersistedTrace>, Error>
    where
        G: Scope,
        G::Timestamp: Lattice;
}

pub trait PersisterV1: Sized {
    type Write: WriteV1;
    type Meta: MetaV1;

    fn create_or_load(
        &mut self,
        id: PersistedId,
    ) -> Result<Persistable<Self::Write, Self::Meta>, Error>;
    fn destroy(&mut self, id: PersistedId) -> Result<(), Error>;
}

pub trait PersisterV2: PersisterV1 {
    type Write: WriteV2;
    type Meta: MetaV2;
}

pub trait Snapshot {
    // returns false when there is no more data
    fn read(&mut self, buf: &mut Vec<((String, String), u64, isize)>) -> bool;
}

pub struct PairSnapshot {
    s1: Box<dyn Snapshot>,
    s2: Box<dyn Snapshot>,
}

impl PairSnapshot {
    pub fn new(s1: Box<dyn Snapshot>, s2: Box<dyn Snapshot>) -> Self {
        PairSnapshot { s1, s2 }
    }
}

impl Snapshot for PairSnapshot {
    fn read(&mut self, buf: &mut Vec<((String, String), u64, isize)>) -> bool {
        self.s1.read(buf) || self.s2.read(buf)
    }
}
