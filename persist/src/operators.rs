//! WIP

use timely::dataflow::{Scope, Stream};

use crate::persister::PersistableV1;
use crate::{Persistable, PersistedStream};

pub trait PersistUnarySync<G: Scope<Timestamp = u64>> {
    fn persist_unary_sync<P: PersistableV1>(self, p: Persistable<P>) -> PersistedStream<G, P>;
}

impl<G: Scope<Timestamp = u64>> PersistUnarySync<G> for Stream<G, ((Vec<u8>, Vec<u8>), u64, i64)> {
    fn persist_unary_sync<P: PersistableV1>(self, p: Persistable<P>) -> PersistedStream<G, P> {
        todo!()
        // let PersistableStream(mut w, mut s) = p;
        // let mut buf = Vec::new();
        // let capture = self.unary_notify(
        //     Pipeline,
        //     "persist_sync_capture",
        //     None,
        //     move |input, output, notificator| {
        //         input.for_each(|time, data| {
        //             data.swap(&mut buf);
        //             w.write_sync(&buf).expect("WIP error handling");
        //             output.session(&time).give_vec(&mut buf);
        //             notificator.notify_at(time.retain());
        //         });
        //         notificator.for_each(|_time, _cnt, _not| {
        //             // WIP do something useful with this. what is the notificator contract again?
        //         });
        //     },
        // );

        // let mut replay = Vec::new();
        // while s.read(&mut replay) {}
        // let replay = replay.into_iter().to_stream(&mut self.scope());
        // // let replay = vec![SnapshotIterator::new(s)].replay_core(&mut self.scope(), None);
        // replay.concat(&capture)
    }
}
