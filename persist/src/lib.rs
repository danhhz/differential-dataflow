//! WIP

use std::error::Error;
use std::sync::{Arc, Mutex};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use crate::storage::sqlite::SQLiteManager;

pub mod storage;
pub mod trace;

pub type SQLitePersistManager = PersistManager<SQLiteManager>;

// WIP the usage of Clone, Arc, and Mutex have gotten out of control

// WIP somehow this is compiling without Send, which is surprising since it's
// passed to timely threads
pub struct PersistManager<R> {
    persister: Arc<Mutex<R>>,
}

impl<R> Clone for PersistManager<R> {
    fn clone(&self) -> Self {
        PersistManager {
            persister: self.persister.clone(),
        }
    }
}

impl<R: Persister> PersistManager<R> {
    pub fn new(persister: R) -> Self {
        PersistManager {
            persister: Arc::new(Mutex::new(persister)),
        }
    }

    pub fn create_or_load(
        &mut self,
        id: u64,
    ) -> Result<(PersistableStream, PersistableMeta), Box<dyn Error>> {
        self.persister.lock().expect("WIP").create_or_load(id)
    }
}

pub trait Persister {
    // WIP it's wierd for this to just return rando boxed traits
    fn create_or_load(
        &mut self,
        id: u64,
    ) -> Result<(PersistableStream, PersistableMeta), Box<dyn Error>>;
}

pub trait PersistedStreamWrite {
    fn write_sync(&mut self, updates: &[(Vec<u8>, u64, i64)]) -> Result<(), Box<dyn Error>>;
}

pub trait PersistedStreamSnapshot {
    // returns false when there is no more data
    fn read(&mut self, buf: &mut Vec<(Vec<u8>, u64, i64)>) -> bool;
}

pub trait PersistedStreamMeta {
    fn advance(&mut self, ts: u64);
    fn allow_compaction(&mut self, ts: u64);
    fn destroy(&mut self) -> Result<(), Box<dyn Error>>;
}

// NB: intentionally not Clone
pub struct PersistableStream(
    pub Box<dyn PersistedStreamWrite>,
    pub Box<dyn PersistedStreamSnapshot>,
);

// NB: intentionally not Clone
pub struct PersistableMeta(pub Box<dyn PersistedStreamMeta>);

pub trait PersistUnarySync {
    fn persist_unary_sync(self, p: PersistableStream) -> Self;
}

impl<G> PersistUnarySync for Stream<G, (Vec<u8>, u64, i64)>
where
    G: Scope<Timestamp = u64>,
{
    fn persist_unary_sync(self, p: PersistableStream) -> Self {
        let PersistableStream(mut w, _s) = p;
        // WIP replay the snapshot into this stream too
        let mut buf = Vec::new();
        self.unary_notify(
            Pipeline,
            "persist_sync",
            None,
            move |input, output, notificator| {
                input.for_each(|time, data| {
                    data.swap(&mut buf);
                    w.write_sync(&buf).expect("WIP error handling");
                    output.session(&time).give_vec(&mut buf);
                    notificator.notify_at(time.retain());
                });
                notificator.for_each(|_time, _cnt, _not| {
                    // WIP do something useful with this. what is the notificator contract again?
                });
            },
        )
    }
}
