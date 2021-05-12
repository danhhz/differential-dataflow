//! WIP

#![allow(dead_code, unused_variables)]

use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use timely::dataflow::{ProbeHandle, Scope, Stream};

use crate::error::Error;
use crate::persister::{PersistableV1, PersistableV2, PersisterV1};
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

// WIP the usage of Clone, Arc, and Mutex have gotten out of control

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

impl<R: PersisterV1> PersistManager<R> {
    pub fn new(persister: R) -> Self {
        PersistManager {
            persister: Arc::new(Mutex::new(persister)),
        }
    }

    pub fn create_or_load(
        &mut self,
        id: PersistedId,
    ) -> Result<Persistable<R::Persistable>, Error> {
        self.persister.lock()?.create_or_load(id)
    }

    // TODO: Should this live on PersistedMeta?
    pub fn destroy(&mut self, id: PersistedId) -> Result<(), Error> {
        self.persister.lock()?.destroy(id)
    }
}

// NB: Intentionally not Clone
pub struct Persistable<P> {
    id: PersistedId,
    p: P,
}

pub struct PersistedStream<G, P>
where
    G: Scope,
{
    persistable: P,
    stream: Stream<G, ((Vec<u8>, Vec<u8>), u64, i64)>,
}

impl<G, P: PersistableV1> PersistedStream<G, P>
where
    G: Scope<Timestamp = u64>,
{
    pub fn meta(&self) -> PersistedMeta {
        todo!()
    }

    // NB: This intentionally consumes self so we only end up with one stream
    // per PersistId per process.
    pub fn into_stream(self) -> Stream<G, ((Vec<u8>, Vec<u8>), u64, i64)> {
        self.stream
    }
}

impl<G, P: PersistableV2> PersistedStream<G, P>
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
{
    pub fn arranged(&self) -> Result<Arranged<G, PersistedTrace>, Error> {
        self.persistable.arranged(self.stream.scope())
    }
}

pub struct PersistedMeta {}

impl PersistedMeta {
    // WIP fn advance(&mut self, ts: u64);

    // WIP fn allow_compaction(&mut self, ts: u64)

    pub fn probe(&self) -> ProbeHandle<u64> {
        todo!()
    }
}

// WIP find homes for these

pub trait Snapshot {
    // returns false when there is no more data
    fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool;
}

struct PairSnapshot {
    s1: Box<dyn Snapshot>,
    s2: Box<dyn Snapshot>,
}

impl Snapshot for PairSnapshot {
    fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool {
        self.s1.read(buf) || self.s2.read(buf)
    }
}

#[cfg(proc_macro)]
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

    use crate::storage::file::{self, FileBuffer};
    use crate::storage::s3::{self, S3Blob};
    use crate::storage::{Blob, BlobPersister, Buffer};
    use crate::{PersistUnarySync, PersistedId, Persister};

    #[test]
    fn persist_unary_sync() -> Result<(), Box<dyn Error>> {
        let id = PersistedId(1);
        let blob = S3Blob::new(s3::Config {})?;
        let buf = FileBuffer::new(file::Config {})?;
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));

        // Initial dataflow
        let (blob1, buf1) = (blob.clone(), buf.clone());
        timely::execute(Config::thread(), move |worker| {
            let mut input = InputSession::new();
            let mut persist = None;

            let probe = worker.dataflow(|scope| {
                let mut probe = ProbeHandle::new();
                let send = send.lock().expect("WIP").clone();
                let mut p = BlobPersister::new(
                    Box::new(blob1.clone()) as Box<dyn Blob>,
                    Box::new(buf1.clone()) as Box<dyn Buffer>,
                )
                .expect("WIP");

                let (stream, meta) = p.create_or_load(id).expect("WIP");
                persist = Some(meta);
                let manages = input
                    .to_collection(scope) // TODO: Get rid of these 2 maps
                    .inner
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, isize)| {
                        ((key, val), ts, diff as i64)
                    })
                    .persist_unary_sync(stream)
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, i64)| {
                        ((key, val), ts, diff as isize)
                    })
                    .probe_with(&mut probe)
                    .as_collection();

                manages.inner.capture_into(send);
                probe
            });
            input.advance_to(0);
            for person in 1..=5 {
                input.insert((
                    format!("k{}", person).into_bytes(),
                    format!("v{}", person).into_bytes(),
                ));
                input.advance_to(person);
                input.flush();
            }
            while probe.less_than(input.time()) {
                worker.step();
            }
            persist.unwrap().0.advance(3);
        })?;

        let first_dataflow = recv.extract();
        // Sanity check
        assert_eq!(first_dataflow.iter().flat_map(|(_, x)| x).count(), 5);

        // Restart dataflow with existing data
        let (blob2, buf2) = (blob.clone(), buf.clone());
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));
        timely::execute(Config::thread(), move |worker| {
            let mut input = InputSession::new();
            worker.dataflow(|scope| {
                let send = send.lock().expect("WIP").clone();
                let mut p = BlobPersister::new(
                    Box::new(blob2.clone()) as Box<dyn Blob>,
                    Box::new(buf2.clone()) as Box<dyn Buffer>,
                )
                .expect("WIP");

                let (stream, _meta) = p.create_or_load(id).expect("WIP");
                let manages = input
                    .to_collection(scope) // TODO: Get rid of these 2 maps
                    .inner
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, isize)| {
                        ((key, val), ts, diff as i64)
                    })
                    .persist_unary_sync(stream)
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, i64)| {
                        ((key, val), ts, diff as isize)
                    })
                    .as_collection();

                manages.inner.capture_into(send);
            });
            input.advance_to(5);
            for person in 6..=8 {
                input.insert((
                    format!("k{}", person).into_bytes(),
                    format!("v{}", person).into_bytes(),
                ));
                input.advance_to(person);
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
        let (send, recv) = mpsc::channel();
        let send = Arc::new(Mutex::new(send));

        timely::execute(Config::thread(), move |worker| {
            let mut input = InputSession::new();
            let (p, mut meta, probe) = worker.dataflow(|scope| {
                let mut p = BlobPersister::new(
                    Box::new(blob.clone()) as Box<dyn Blob>,
                    Box::new(buf.clone()) as Box<dyn Buffer>,
                )
                .expect("WIP");
                let (stream, meta) = p.create_or_load(id).expect("WIP");

                let probe = input
                    .to_collection(scope)
                    .inner
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, isize)| {
                        ((key, val), ts, diff as i64)
                    })
                    .persist_unary_sync(stream)
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, i64)| {
                        ((key, val), ts, diff as isize)
                    })
                    .as_collection()
                    .probe();
                (p, meta, probe)
            });

            input.advance_to(0);
            for person in 0..10 {
                input.insert((
                    (person / 2).to_string().into_bytes(),
                    (person).to_string().into_bytes(),
                ));
            }
            input.advance_to(10);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            meta.0.advance(10);
            assert!(blob.entries() > 0);

            worker.dataflow(|scope| {
                let send = send.lock().expect("WIP").clone();
                let manages_arranged = p.arranged(scope.clone(), 1).expect("WIP");
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
            let (x, y, z) = (
                x.to_string().into_bytes(),
                y.to_string().into_bytes(),
                z.to_string().into_bytes(),
            );
            // Swap z and y to match the dd book since we had the wrong
            // thing arranged for join_core.
            ((x, (z, y)), t as u64, r as isize)
        })
        .collect::<Vec<_>>();
        assert_eq!(format!("{:?}", captured), format!("{:?}", expected));

        Ok(())
    }
}
