//! WIP

use std::error::Error;
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Concat, Operator, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::PartialOrder;

use crate::abom::AbomonatedBatch;
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

    fn arranged<G>(&self, scope: G, id: u64) -> Arranged<G, PersistedTraceReader>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord;
}

pub trait PersistedStreamWrite {
    fn write_sync(
        &mut self,
        updates: &[((Vec<u8>, Vec<u8>), u64, i64)],
    ) -> Result<(), Box<dyn Error>>;
}

pub trait PersistedStreamSnapshot {
    // returns false when there is no more data
    fn read(&mut self, buf: &mut Vec<((Vec<u8>, Vec<u8>), u64, i64)>) -> bool;
}

pub trait PersistedStreamMeta {
    fn advance(&mut self, ts: u64);
    fn allow_compaction(&mut self, ts: u64);
    fn destroy(&mut self) -> Result<(), Box<dyn Error>>;
}

#[derive(Clone)]
pub struct PersistedTraceReader {
    batches: Vec<AbomonatedBatch>,
    logical_compaction: Antichain<u64>,
    physical_compaction: Antichain<u64>,
}

impl PersistedTraceReader {
    fn new(batches: Vec<AbomonatedBatch>) -> Self {
        PersistedTraceReader {
            batches,
            logical_compaction: Antichain::new(),
            physical_compaction: Antichain::new(),
        }
    }
}

impl TraceReader for PersistedTraceReader {
    type Key = Vec<u8>;

    type Val = Vec<u8>;

    type Time = u64;

    type R = isize;

    type Batch = AbomonatedBatch;

    type Cursor = CursorList<
        Vec<u8>,
        Vec<u8>,
        u64,
        isize,
        <AbomonatedBatch as BatchReader<Vec<u8>, Vec<u8>, u64, isize>>::Cursor,
    >;

    fn cursor_through(
        &mut self,
        upper: timely::progress::frontier::AntichainRef<Self::Time>,
    ) -> Option<(
        Self::Cursor,
        <Self::Cursor as Cursor<Self::Key, Self::Val, Self::Time, Self::R>>::Storage,
    )> {
        // WIP check this filter condition. also sanity check that the batches
        // line up with no holes
        let storage = self
            .batches
            .iter()
            .filter(|b| PartialOrder::less_equal(&b.upper().borrow(), &upper))
            .cloned()
            .collect::<Vec<_>>();
        let cursors = storage.iter().map(|b| b.cursor()).collect();
        Some((CursorList::new(cursors, &storage), storage))
    }

    fn set_logical_compaction(
        &mut self,
        frontier: timely::progress::frontier::AntichainRef<Self::Time>,
    ) {
        self.logical_compaction.clear();
        self.logical_compaction.extend(frontier.iter().cloned());
    }

    fn get_logical_compaction(&mut self) -> timely::progress::frontier::AntichainRef<Self::Time> {
        self.logical_compaction.borrow()
    }

    fn set_physical_compaction(
        &mut self,
        frontier: timely::progress::frontier::AntichainRef<Self::Time>,
    ) {
        debug_assert!(timely::PartialOrder::less_equal(
            &self.physical_compaction.borrow(),
            &frontier
        ));
        self.physical_compaction.clear();
        self.physical_compaction.extend(frontier.iter().cloned());
    }

    fn get_physical_compaction(&mut self) -> timely::progress::frontier::AntichainRef<Self::Time> {
        self.physical_compaction.borrow()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        for batch in self.batches.iter() {
            f(batch)
        }
    }
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

impl<G> PersistUnarySync for Stream<G, ((Vec<u8>, Vec<u8>), u64, i64)>
where
    G: Scope<Timestamp = u64>,
{
    fn persist_unary_sync(self, p: PersistableStream) -> Self {
        let PersistableStream(mut w, mut s) = p;
        let mut buf = Vec::new();
        let capture = self.unary_notify(
            Pipeline,
            "persist_sync_capture",
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
        );

        let mut replay = Vec::new();
        while s.read(&mut replay) {}
        let replay = replay.into_iter().to_stream(&mut self.scope());
        // let replay = vec![SnapshotIterator::new(s)].replay_core(&mut self.scope(), None);
        replay.concat(&capture)
    }
}

// struct SnapshotIterator {
//     s: Box<dyn PersistedStreamSnapshot>,
//     e: Option<Event<u64, (Vec<u8>, u64, i64)>>,
// }

// impl SnapshotIterator {
//     fn new(s: Box<dyn PersistedStreamSnapshot>) -> Self {
//         SnapshotIterator { s, e: None }
//     }
// }

// impl EventIterator<u64, (Vec<u8>, u64, i64)> for SnapshotIterator {
//     fn next(&mut self) -> Option<&Event<u64, (Vec<u8>, u64, i64)>> {
//         let mut buf = vec![];
//         loop {
//             let more = self.s.read(&mut buf);
//             eprintln!("WIP {}: {:?}", more, &buf);
//             if buf.is_empty() && !more {
//                 eprintln!("WIP all done");
//                 return None;
//             } else if buf.is_empty() {
//                 continue;
//             }
//             self.e = Some(Event::Messages(0, buf));
//             return Some(self.e.as_ref().unwrap());
//         }
//     }
// }

// WIP I couldn't get the types happy with the existing stuff so I had to make
// these unfortunate wrappers
mod abom {
    use std::sync::Arc;

    use abomonation::abomonated::Abomonated;
    use differential_dataflow::trace::abomonated_blanket_impls::AbomonatedBatchCursor;
    use differential_dataflow::trace::implementations::ord::OrdValBatch;
    use differential_dataflow::trace::{BatchReader, Cursor};

    pub struct AbomonatedBatch(
        pub Arc<Abomonated<OrdValBatch<Vec<u8>, Vec<u8>, u64, i64, usize>, Vec<u8>>>,
    );

    impl Clone for AbomonatedBatch {
        fn clone(&self) -> Self {
            AbomonatedBatch(self.0.clone())
        }
    }

    impl BatchReader<Vec<u8>, Vec<u8>, u64, isize> for AbomonatedBatch {
        type Cursor = AbomonatedCursor;

        fn cursor(&self) -> Self::Cursor {
            AbomonatedCursor(self.0.cursor())
        }

        fn len(&self) -> usize {
            self.0.len()
        }

        fn description(&self) -> &differential_dataflow::trace::Description<u64> {
            self.0.description()
        }
    }

    pub struct AbomonatedCursor(
        AbomonatedBatchCursor<
            Vec<u8>,
            Vec<u8>,
            u64,
            i64,
            OrdValBatch<Vec<u8>, Vec<u8>, u64, i64, usize>,
        >,
    );

    impl Cursor<Vec<u8>, Vec<u8>, u64, isize> for AbomonatedCursor {
        type Storage = AbomonatedBatch;

        fn key_valid(&self, storage: &Self::Storage) -> bool {
            self.0.key_valid(&storage.0)
        }

        fn val_valid(&self, storage: &Self::Storage) -> bool {
            self.0.val_valid(&storage.0)
        }

        fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Vec<u8> {
            self.0.key(&storage.0)
        }

        fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Vec<u8> {
            self.0.val(&storage.0)
        }

        fn map_times<L: FnMut(&u64, &isize)>(&mut self, storage: &Self::Storage, mut logic: L) {
            self.0
                .map_times(&storage.0, |t, r| logic(t, &(*r as isize)))
        }

        fn step_key(&mut self, storage: &Self::Storage) {
            self.0.step_key(&storage.0)
        }

        fn seek_key(&mut self, storage: &Self::Storage, key: &Vec<u8>) {
            self.0.seek_key(&storage.0, key)
        }

        fn step_val(&mut self, storage: &Self::Storage) {
            self.0.step_val(&storage.0)
        }

        fn seek_val(&mut self, storage: &Self::Storage, val: &Vec<u8>) {
            self.0.seek_val(&storage.0, val)
        }

        fn rewind_keys(&mut self, storage: &Self::Storage) {
            self.0.rewind_keys(&storage.0)
        }

        fn rewind_vals(&mut self, storage: &Self::Storage) {
            self.0.rewind_vals(&storage.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::{mpsc, Arc, Mutex};

    use differential_dataflow::input::InputSession;
    use differential_dataflow::operators::Join;
    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::capture::extract::Extract;
    use timely::dataflow::operators::{Capture, Map, Probe};
    use timely::dataflow::ProbeHandle;
    use timely::Config;

    use crate::storage::file::{self, FileBuffer};
    use crate::storage::s3::{self, S3Blob};
    use crate::storage::{Blob, BlobPersister, Buffer};
    use crate::{PersistUnarySync, Persister};

    #[test]
    fn persist_unary_sync() -> Result<(), Box<dyn Error>> {
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

                let (stream, meta) = p.create_or_load(1).expect("WIP");
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

                let (stream, _meta) = p.create_or_load(1).expect("WIP");
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
        let blob = S3Blob::new(s3::Config {})?;
        let buf = FileBuffer::new(file::Config {})?;
        // let (send, recv) = mpsc::channel();
        // let send = Arc::new(Mutex::new(send));

        timely::execute(Config::thread(), move |worker| {
            let mut input = InputSession::new();
            worker.dataflow(|scope| {
                let mut p = BlobPersister::new(
                    Box::new(blob.clone()) as Box<dyn Blob>,
                    Box::new(buf.clone()) as Box<dyn Buffer>,
                )
                .expect("WIP");
                let (stream, _meta) = p.create_or_load(1).expect("WIP");

                let manages = input
                    .to_collection(scope)
                    .inner
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, isize)| {
                        ((key, val), ts, diff as i64)
                    })
                    .persist_unary_sync(stream)
                    .map(|((key, val), ts, diff): ((Vec<u8>, Vec<u8>), u64, i64)| {
                        ((key, val), ts, diff as isize)
                    })
                    .as_collection();
                let manages_arranged = p.arranged(scope.clone(), 1);
                let managed = manages.map(|(m2, m1)| (m1, m2));
                manages_arranged
                    .join(&managed)
                    .inspect(|x| println!("{:?}", x));
            });
            let size = 10;
            input.advance_to(0);
            for person in 0..size {
                input.insert((
                    (person / 2).to_string().into_bytes(),
                    (person).to_string().into_bytes(),
                ));
            }
        })?;
        Ok(())
    }
}
