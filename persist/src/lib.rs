//! WIP

#![allow(dead_code, unused_variables)]

use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::Collection;
use timely::dataflow::{ProbeHandle, Scope, Stream};

use crate::error::Error;
use crate::persister::{MetaV1, MetaV2, PersisterV1};
use crate::storage::sqlite::SQLiteManager;
use crate::trace::PersistedTrace;

pub mod error;
pub mod operators;
pub mod persister;
pub mod storage;
pub mod trace;

pub type SQLitePersistManager = PersistManager<SQLiteManager>;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PersistedId(pub u64);

pub struct PersistManager<P> {
    persister: Arc<Mutex<P>>,
}

// We don't get the automatically derived Send and Sync because of the type
// parameter.
unsafe impl<P> Send for PersistManager<P> {}
unsafe impl<P> Sync for PersistManager<P> {}

impl<P> Clone for PersistManager<P> {
    fn clone(&self) -> Self {
        PersistManager {
            persister: self.persister.clone(),
        }
    }
}

impl<P: PersisterV1> PersistManager<P> {
    pub fn new(persister: P) -> Self {
        PersistManager {
            persister: Arc::new(Mutex::new(persister)),
        }
    }

    pub fn create_or_load(
        &mut self,
        id: PersistedId,
    ) -> Result<Persistable<P::Write, P::Meta>, Error> {
        self.persister.lock()?.create_or_load(id)
    }

    // TODO: Should this live on PersistedMeta?
    pub fn destroy(&mut self, id: PersistedId) -> Result<(), Error> {
        self.persister.lock()?.destroy(id)
    }
}

// NB: Intentionally not Clone since it's an exclusivity token.
pub struct Persistable<W, M> {
    id: PersistedId,
    write: W,
    meta: M,
}

impl<W, M> Persistable<W, M> {
    fn consume(self) -> (W, M) {
        (self.write, self.meta)
    }
}

#[derive(Clone)]
pub struct PersistedMeta<M> {
    meta: M,
    // NB: This is here instead of a method on ReadV1 so that the operator can
    // stop bothering to update it if no one cares (via holding a Weak ref for
    // the probe core).
    probe: ProbeHandle<u64>,
}

// TODO: At the moment, PersistedMeta basically just forwards methods to a
// MetaV1 or MetaV2. Is it worth it for the lazy probe? Maybe there's a better
// way to do that.
impl<M: MetaV1> PersistedMeta<M> {
    pub fn probe(&self) -> ProbeHandle<u64> {
        self.probe.clone()
    }

    pub fn allow_compaction(&mut self, ts: u64) {
        self.meta.allow_compaction(ts)
    }
}

impl<M: MetaV2> PersistedMeta<M> {
    pub fn advance(&mut self, ts: u64) {
        self.meta.advance(ts)
    }

    pub fn arranged<G>(&self, scope: G) -> Result<Arranged<G, crate::trace::PersistedTrace>, Error>
    where
        G: Scope,
        G::Timestamp: Lattice,
    {
        self.meta.arranged(scope)
    }
}

pub struct PersistedStream<G, M>
where
    G: Scope,
{
    meta: PersistedMeta<M>,
    stream: Stream<G, ((String, String), u64, isize)>,
}

impl<G, M: MetaV1> PersistedStream<G, M>
where
    G: Scope<Timestamp = u64>,
{
    pub fn meta(&self) -> &PersistedMeta<M> {
        &self.meta
    }

    // NB: This intentionally consumes self so we only end up with one stream
    // per PersistId per process.
    pub fn into_stream(self) -> Stream<G, ((String, String), u64, isize)> {
        self.stream
    }
}

pub struct PersistedCollection<G, M>
where
    G: Scope<Timestamp = u64>,
{
    meta: PersistedMeta<M>,
    collection: Collection<G, (String, String), isize>,
}

impl<G, M: MetaV1> PersistedCollection<G, M>
where
    G: Scope<Timestamp = u64>,
{
    pub fn meta(&self) -> &PersistedMeta<M> {
        &self.meta
    }

    // NB: This intentionally consumes self so we only end up with one stream
    // per PersistId per process.
    pub fn into_collection(self) -> Collection<G, (String, String), isize> {
        self.collection
    }

    pub fn into_stream(self) -> PersistedStream<G, M> {
        PersistedStream {
            meta: self.meta,
            stream: self.collection.inner,
        }
    }
}

impl<G, M: MetaV2> PersistedCollection<G, M>
where
    G: Scope<Timestamp = u64>,
{
    pub fn arranged(&self) -> Result<Arranged<G, PersistedTrace>, Error> {
        self.meta.meta.arranged(self.collection.inner.scope())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{mpsc, Arc, Mutex};

    use differential_dataflow::input::InputSession;
    use differential_dataflow::operators::Join;
    use differential_dataflow::trace::cursor::CursorDebug;
    use differential_dataflow::trace::BatchReader;
    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::capture::extract::Extract;
    use timely::dataflow::operators::{Capture, Map, Probe};
    use timely::dataflow::ProbeHandle;
    use timely::Config;

    use crate::operators::PersistCollectionSync;
    use crate::storage::file::{self, FileBuffer};
    use crate::storage::s3::{self, S3Blob};
    use crate::storage::sqlite::SQLiteManager;
    use crate::storage::{Blob, BlobPersister, Buffer};
    use crate::{PersistManager, PersistedId};

    // TODO: These tests feel super clunkly

    #[test]
    fn persist_collection_sync() -> Result<(), Box<dyn Error>> {
        let p = PersistManager::new(SQLiteManager::new());

        // Initial dataflow
        let p2 = p.clone();
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        timely::execute(Config::thread(), move |worker| {
            let mut input = InputSession::new();
            let mut p2 = p2.clone();
            let probe = worker.dataflow(|scope| {
                let mut probe = ProbeHandle::new();
                let send = send.lock().expect("WIP").clone();

                let persister = p2.create_or_load(PersistedId(1)).expect("WIP");
                input
                    .to_collection(scope)
                    .persist_collection_sync(persister)
                    .into_collection()
                    .inner
                    .probe_with(&mut probe)
                    .capture_into(send);
                probe
            });
            input.advance_to(0);
            for person in 1..=5 {
                input.insert((format!("k{}", person), format!("v{}", person)));
                input.advance_to(person);
                input.flush();
            }
            while probe.less_than(input.time()) {
                worker.step();
            }
        })?;

        let first_dataflow = recv.extract();
        // Sanity check
        assert_eq!(first_dataflow.iter().flat_map(|(_, x)| x).count(), 5);

        // Restart dataflow with existing data
        let p2 = p.clone();
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        timely::execute(Config::thread(), move |worker| {
            let mut input = InputSession::new();

            let mut p2 = p2.clone();
            let probe = worker.dataflow(|scope| {
                let mut probe = ProbeHandle::new();
                let send = send.lock().expect("WIP").clone();

                let persister = p2.create_or_load(PersistedId(1)).expect("WIP");
                input
                    .to_collection(scope)
                    .persist_collection_sync(persister)
                    .into_collection()
                    .inner
                    .probe_with(&mut probe)
                    .capture_into(send);
                probe
            });
            input.advance_to(0);
            for person in 6..=8 {
                input.insert((format!("k{}", person), format!("v{}", person)));
                input.advance_to(person);
                input.flush();
            }
            while probe.less_than(input.time()) {
                worker.step();
            }
        })?;

        let second_dataflow = recv.extract();
        assert_eq!(second_dataflow.iter().flat_map(|(_, x)| x).count(), 8);

        Ok(())
    }

    #[test]
    fn arrangement() -> Result<(), Box<dyn Error>> {
        let id = PersistedId(1);
        let blob = S3Blob::new(s3::Config {})?;
        let buf = FileBuffer::new(file::Config {})?;
        let p = PersistManager::new(BlobPersister::new(
            Box::new(blob.clone()) as Box<dyn Blob>,
            Box::new(buf.clone()) as Box<dyn Buffer>,
        )?);

        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        timely::execute(Config::thread(), move |worker| {
            let mut p = p.clone();
            let mut input = InputSession::new();
            let (mut meta, probe) = worker.dataflow(|scope| {
                let persister = p.create_or_load(PersistedId(1)).expect("WIP");

                let c = input
                    .to_collection(scope)
                    .persist_collection_sync(persister);
                let meta = c.meta().clone();
                let probe = c.into_collection().probe();
                (meta, probe)
            });

            input.advance_to(0);
            for person in 0..10 {
                input.insert(((person / 2).to_string(), (person).to_string()));
            }
            input.advance_to(10);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            meta.advance(10);
            assert!(blob.entries() > 0);

            worker.dataflow(|scope| {
                let send = send.lock().expect("WIP").clone();
                let manages_arranged = meta.arranged(scope.clone()).expect("WIP");
                let manages = manages_arranged
                    .stream
                    .flat_map(|b| {
                        let mut cursor = b.cursor();
                        cursor.to_vec(&b).into_iter().flat_map(|((k, v), trs)| {
                            trs.into_iter()
                                .map(move |(t, r)| ((k.clone(), v.clone()), t, r))
                        })
                    })
                    .as_collection();
                let managed = manages.map(|(m2, m1)| (m1, m2));
                manages_arranged.join(&managed).inner.capture_into(send);
            });
        })?;

        let captured = recv.extract();
        let captured = captured.iter().flat_map(|(_, x)| x).collect::<Vec<_>>();
        let expected = vec![
            (("0", ("0", "0")), 0, 1),
            (("0", ("0", "1")), 0, 1),
            (("1", ("0", "2")), 0, 1),
            (("1", ("0", "3")), 0, 1),
            (("2", ("1", "4")), 0, 1),
            (("2", ("1", "5")), 0, 1),
            (("3", ("1", "6")), 0, 1),
            (("3", ("1", "7")), 0, 1),
            (("4", ("2", "8")), 0, 1),
            (("4", ("2", "9")), 0, 1),
        ]
        .into_iter()
        .map(|((x, (y, z)), t, r)| {
            let (x, y, z) = (x.to_string(), y.to_string(), z.to_string());
            // Swap z and y to match the dd book since we had the wrong
            // stream arranged for the opposite join_core argument ordering.
            ((x, (z, y)), t as u64, r as isize)
        })
        .collect::<Vec<_>>();
        assert_eq!(format!("{:?}", captured), format!("{:?}", expected));

        Ok(())
    }
}
