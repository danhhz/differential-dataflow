//! WIP

use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Concat, Operator, ToStream};
use timely::dataflow::{ProbeHandle, Scope, Stream};

use crate::persister::{MetaV1, Snapshot, WriteV1};
use crate::{Persistable, PersistedCollection, PersistedMeta, PersistedStream};

pub trait PersistUnarySync<G: Scope<Timestamp = u64>> {
    fn persist_unary_sync<W: WriteV1 + 'static, M: MetaV1 + 'static>(
        self,
        p: Persistable<W, M>,
    ) -> PersistedStream<G, M>;
}

impl<G: Scope<Timestamp = u64>> PersistUnarySync<G> for Stream<G, ((String, String), u64, isize)> {
    fn persist_unary_sync<W: WriteV1 + 'static, M: MetaV1 + 'static>(
        self,
        p: Persistable<W, M>,
    ) -> PersistedStream<G, M> {
        let (mut write, meta) = p.consume();
        let mut snap = meta.snapshot().expect("WIP");
        let probe = ProbeHandle::new();
        // TODO: Make this a Weak reference so we can stop updating it if no-one
        // cares.
        let _probe_frontier = probe.clone();

        let mut buf = Vec::new();
        let capture = self.unary_notify(
            Pipeline,
            "persist_sync_capture",
            None,
            move |input, output, notificator| {
                input.for_each(|time, data| {
                    data.swap(&mut buf);
                    write.write_sync(&buf).expect("WIP error handling");
                    output.session(&time).give_vec(&mut buf);
                    notificator.notify_at(time.retain());
                });
                notificator.for_each(|_time, _cnt, _not| {
                    // WIP do something useful with this. what is the notificator contract again?

                    // WIP update probe
                });
            },
        );

        let meta = PersistedMeta { meta, probe };

        let mut replay = Vec::new();
        while snap.read(&mut replay) {}
        let replay = replay.into_iter().to_stream(&mut self.scope());
        // let replay = vec![SnapshotIterator::new(s)].replay_core(&mut self.scope(), None);
        let stream = replay.concat(&capture);

        PersistedStream { meta, stream }
    }
}

pub trait PersistCollectionSync<G: Scope<Timestamp = u64>> {
    fn persist_collection_sync<W: WriteV1 + 'static, M: MetaV1 + 'static>(
        self,
        p: Persistable<W, M>,
    ) -> PersistedCollection<G, M>;
}

impl<G: Scope<Timestamp = u64>> PersistCollectionSync<G>
    for Collection<G, (String, String), isize>
{
    fn persist_collection_sync<W: WriteV1 + 'static, M: MetaV1 + 'static>(
        self,
        p: Persistable<W, M>,
    ) -> PersistedCollection<G, M> {
        let PersistedStream { meta, stream } = self.inner.persist_unary_sync(p);
        PersistedCollection {
            meta,
            collection: stream.as_collection(),
        }
    }
}
