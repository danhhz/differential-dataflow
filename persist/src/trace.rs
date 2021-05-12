//! Persistent implementations of trace datastructures.

use std::cmp;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use capnp::serialize;
use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::{BatchReader, Cursor, Description, TraceReader};
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::PartialOrder;

use crate::storage::BlobPersisterCore;
use crate::trace::abom::AbomonatedBatch;

#[derive(Clone)]
pub struct PersistedTrace {
    core: Arc<Mutex<BlobPersisterCore>>,
    logical_compaction: Antichain<u64>,
    physical_compaction: Antichain<u64>,
}

impl PersistedTrace {
    pub(crate) fn new(core: Arc<Mutex<BlobPersisterCore>>) -> Self {
        PersistedTrace {
            core,
            logical_compaction: Antichain::from_elem(u64::minimum()),
            physical_compaction: Antichain::from_elem(u64::minimum()),
        }
    }
}

impl TraceReader for PersistedTrace {
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
        let mut core = self.core.lock().expect("WIP");
        // WIP unfortunate extra clone
        let batch_keys = core.blob_meta.batch_keys.clone();
        // WIP check this filter condition. also sanity check that the batches
        // line up with no holes
        let storage = batch_keys
            .iter()
            .map(|key| core.get_blob(key).expect("WIP").expect("WIP"))
            .filter(|b| PartialOrder::less_equal(&b.upper().borrow(), &upper))
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
        let mut core = self.core.lock().expect("WIP");
        // WIP unfortunate extra clone
        let batch_keys = core.blob_meta.batch_keys.clone();
        let batches = batch_keys
            .iter()
            .map(|key| core.get_blob(key).expect("WIP").expect("WIP"));
        for batch in batches {
            f(&batch)
        }
    }
}

// WIP I couldn't get the types happy with the existing stuff so I had to make
// these unfortunate wrappers
pub(crate) mod abom {
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

// CAPNP EXPERIMENTS BELOW

pub mod capnpgen {
    mod trace_capnp {
        include!(concat!(env!("OUT_DIR"), "/trace_capnp.rs"));
    }
    pub use trace_capnp::*;
}

pub struct PBatch<'b> {
    d: Description<u64>,
    tuples: capnp::struct_list::Reader<'b, capnpgen::tuple::Owned>,
}

impl<'b> PBatch<'b> {
    pub fn from_reader(b: capnpgen::batch::Reader<'b>) -> Result<Self, Box<dyn std::error::Error>> {
        let d = Description::new(
            Antichain::from_elem(b.get_lower()),
            Antichain::from_elem(b.get_upper()),
            Antichain::from_elem(b.get_since()),
        );
        let tuples = b.get_tuples()?;
        Ok(PBatch { d, tuples })
    }
}

impl<'b> BatchReader<[u8], [u8], u64, i64> for PBatch<'b> {
    type Cursor = PCursor<'b>;

    fn cursor(&self) -> Self::Cursor {
        PCursor {
            off: 0,
            _pd: std::marker::PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.tuples.len() as usize
    }

    fn description(&self) -> &Description<u64> {
        &self.d
    }
}

pub struct PCursor<'b> {
    off: u32,
    _pd: std::marker::PhantomData<&'b ()>,
}

impl<'b> PCursor<'b> {
    fn key_raw<'a>(off: u32, storage: &'a PBatch) -> Option<&'a [u8]> {
        let key = if off < storage.tuples.len() {
            storage.tuples.get(off).get_key().ok()
        } else {
            None
        };
        key
    }

    fn val_raw<'a>(off: u32, storage: &'a PBatch) -> Option<&'a [u8]> {
        let val = if off < storage.tuples.len() {
            storage.tuples.get(off).get_val().ok()
        } else {
            None
        };
        val
    }
}

impl<'b> Cursor<[u8], [u8], u64, i64> for PCursor<'b> {
    type Storage = PBatch<'b>;

    fn key_valid(&self, storage: &Self::Storage) -> bool {
        PCursor::key_raw(self.off, storage).is_some()
    }

    fn val_valid(&self, storage: &Self::Storage) -> bool {
        PCursor::val_raw(self.off, storage).is_some()
    }

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a [u8] {
        let key = PCursor::key_raw(self.off, storage).expect("invalid key");
        key
    }

    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a [u8] {
        let val = PCursor::val_raw(self.off, storage).expect("invalid val");
        val
    }

    fn map_times<L: FnMut(&u64, &i64)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = PCursor::key_raw(self.off, storage).expect("invalid key");
        let val = PCursor::val_raw(self.off, storage).expect("invalid val");
        loop {
            let tuple = storage.tuples.get(self.off);
            logic(&tuple.get_ts(), &tuple.get_diff());
            if PCursor::val_raw(self.off + 1, storage) == Some(val)
                && PCursor::key_raw(self.off + 1, storage) == Some(key)
            {
                self.off += 1;
            } else {
                break;
            }
        }
    }

    fn step_key(&mut self, storage: &Self::Storage) {
        if !self.key_valid(storage) {
            return;
        }
        let key = PCursor::key_raw(self.off, storage).expect("invalid key");
        self.off += 1;
        while PCursor::key_raw(self.off, storage) == Some(key) {
            self.off += 1;
        }
    }

    fn seek_key(&mut self, storage: &Self::Storage, key: &[u8]) {
        // Copied with modifications from
        // https://doc.rust-lang.org/std/primitive.slice.html#method.binary_search_by
        let mut size = storage.tuples.len();
        if size == 0 {
            return;
        }
        let mut base = 0;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;
            // SAFETY: the call is made safe by the following inconstants:
            // - `mid >= 0`: by definition
            // - `mid < size`: `mid = size / 2 + size / 4 + size / 8 ...`
            let mid_key = PCursor::key_raw(mid, storage).expect("invalid key");
            let cmp = mid_key.cmp(key);
            base = if cmp == cmp::Ordering::Greater {
                base
            } else {
                mid
            };
            size -= half;
        }
        // SAFETY: base is always in [0, size) because base <= mid.
        self.off = base;
        let base_key = PCursor::key_raw(self.off, storage).expect("invalid key");
        let cmp = base_key.cmp(key);
        if cmp == cmp::Ordering::Equal {
            // We found the key but we're not guaranteed to have the first one,
            // so rewind
            self.rewind_vals(storage);
        } else {
            // Key not found, set to an invalid offset.
            self.off = storage.tuples.len();
        }
    }

    fn step_val(&mut self, _storage: &Self::Storage) {
        self.off += 1;
    }

    fn seek_val(&mut self, _storage: &Self::Storage, _val: &[u8]) {
        todo!()
    }

    fn rewind_keys(&mut self, _storage: &Self::Storage) {
        self.off = 0;
    }

    fn rewind_vals(&mut self, storage: &Self::Storage) {
        // TODO: Do this with binary search. This becomes unnecessary if we do
        // the non-redundant key storage TODO in trace.capnp.
        let key = PCursor::key_raw(self.off, storage).expect("invalid key");
        while self.off > 0 {
            let prev_off = self.off - 1;
            if PCursor::key_raw(prev_off, storage) == Some(key) {
                self.off = prev_off;
            } else {
                break;
            }
        }
    }
}

pub struct ColumnarBatch<'a> {
    len: u32,
    d: Description<u64>,
    key_data: &'a [u8],
    key_data_offsets: ::capnp::primitive_list::Reader<'a, u32>,
    val_data: &'a [u8],
    val_data_offsets: ::capnp::primitive_list::Reader<'a, u32>,
    timestamps: ::capnp::primitive_list::Reader<'a, u64>,
    diffs: ::capnp::primitive_list::Reader<'a, i64>,

    key_idx_by_idx: ::capnp::primitive_list::Reader<'a, u32>,
    val_idx_by_idx: ::capnp::primitive_list::Reader<'a, u32>,
}

impl<'a> ColumnarBatch<'a> {
    pub fn from_reader(
        b: capnpgen::batch_columnar::Reader<'a>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let d = Description::new(
            Antichain::from_elem(b.get_lower()),
            Antichain::from_elem(b.get_upper()),
            Antichain::from_elem(b.get_since()),
        );
        let diffs = b.get_diffs()?;
        Ok(ColumnarBatch {
            len: diffs.len(),
            d,
            key_data: b.get_key_data()?,
            key_data_offsets: b.get_key_data_offsets()?,
            val_data: b.get_val_data()?,
            val_data_offsets: b.get_val_data_offsets()?,
            timestamps: b.get_timestamps()?,
            diffs: diffs,
            key_idx_by_idx: b.get_key_idx_by_idx()?,
            val_idx_by_idx: b.get_val_idx_by_idx()?,
        })
    }

    fn key_raw(&self, idx: u32) -> &'a [u8] {
        let key_idx = self.key_idx_by_idx.get(idx);
        &self.key_data[self.key_data_offsets.get(key_idx) as usize
            ..self.key_data_offsets.get(key_idx + 1) as usize]
    }
    fn val_raw(&self, idx: u32) -> &'a [u8] {
        let val_idx = self.val_idx_by_idx.get(idx);
        &self.val_data[self.val_data_offsets.get(val_idx) as usize
            ..self.val_data_offsets.get(val_idx + 1) as usize]
    }
}

impl<'a> IntoIterator for ColumnarBatch<'a> {
    type Item = (&'a [u8], &'a [u8], u64, i64);

    type IntoIter = ColumnarBatchIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ColumnarBatchIter { idx: 0, b: self }
    }
}

pub struct ColumnarBatchIter<'a> {
    idx: u32,
    b: ColumnarBatch<'a>,
}

impl<'a> Iterator for ColumnarBatchIter<'a> {
    type Item = (&'a [u8], &'a [u8], u64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.b.diffs.len() {
            return None;
        }
        let key = self.b.key_raw(self.idx);
        let val = self.b.val_raw(self.idx);
        let ts = self.b.timestamps.get(self.idx);
        let diff = self.b.diffs.get(self.idx);
        self.idx += 1;
        Some((key, val, ts, diff))
    }
}

impl<'a> BatchReader<[u8], [u8], u64, i64> for ColumnarBatch<'a> {
    type Cursor = ColumnarBatchCursor<'a>;

    fn cursor(&self) -> Self::Cursor {
        ColumnarBatchCursor {
            idx: 0,
            phantom: PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.diffs.len() as usize
    }

    fn description(&self) -> &Description<u64> {
        &self.d
    }
}

pub struct ColumnarBatchCursor<'b> {
    idx: u32,
    phantom: PhantomData<&'b ()>,
}

impl<'b> Cursor<[u8], [u8], u64, i64> for ColumnarBatchCursor<'b> {
    type Storage = ColumnarBatch<'b>;

    fn key_valid(&self, storage: &Self::Storage) -> bool {
        self.idx < storage.len
    }

    fn val_valid(&self, storage: &Self::Storage) -> bool {
        self.idx < storage.len
    }

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a [u8] {
        storage.key_raw(self.idx)
    }

    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a [u8] {
        storage.val_raw(self.idx)
    }

    fn map_times<L: FnMut(&u64, &i64)>(&mut self, _storage: &Self::Storage, _logic: L) {
        todo!()
    }

    fn step_key(&mut self, _storage: &Self::Storage) {
        self.idx += 1;
    }

    fn seek_key(&mut self, _storage: &Self::Storage, _key: &[u8]) {
        todo!()
    }

    fn step_val(&mut self, _storage: &Self::Storage) {
        todo!()
    }

    fn seek_val(&mut self, _storage: &Self::Storage, _val: &[u8]) {
        todo!()
    }

    fn rewind_keys(&mut self, _storage: &Self::Storage) {
        todo!()
    }

    fn rewind_vals(&mut self, _storage: &Self::Storage) {
        todo!()
    }
}

pub struct ColumnarBatchBuilder {
    key_data: Vec<u8>,
    key_data_offsets: Vec<u32>,
    val_data: Vec<u8>,
    val_data_offsets: Vec<u32>,
    timestamps: Vec<u64>,
    diffs: Vec<i64>,

    key_idx_by_idx: Vec<u32>,
    val_idx_by_idx: Vec<u32>,
}

fn fill_capnp_list<'a, T>(b: &mut ::capnp::primitive_list::Builder<'a, T>, xs: &[T])
where
    T: ::capnp::private::layout::PrimitiveElement + Copy,
{
    // TODO: Seriously? This is how I have to do this?
    for (idx, x) in xs.iter().enumerate() {
        b.set(idx as u32, *x);
    }
}

impl ColumnarBatchBuilder {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(cap: usize) -> Self {
        // TODO: This isn't the right way to use cap with keydata/valdata. Given
        // that we've already sorted these, we should be able to right size them
        // with some plumbing.
        let mut ret = ColumnarBatchBuilder {
            key_data: Vec::with_capacity(cap),
            key_data_offsets: Vec::with_capacity(cap + 1),
            val_data: Vec::with_capacity(cap),
            val_data_offsets: Vec::with_capacity(cap + 1),
            timestamps: Vec::with_capacity(cap),
            diffs: Vec::with_capacity(cap),
            key_idx_by_idx: Vec::with_capacity(cap),
            val_idx_by_idx: Vec::with_capacity(cap),
        };
        ret.key_data_offsets.push(0);
        ret.val_data_offsets.push(0);
        ret
    }

    fn key_last(&self) -> Option<&[u8]> {
        if self.key_data_offsets.len() < 2 {
            None
        } else {
            Some(
                &self.key_data[self.key_data_offsets[self.key_data_offsets.len() - 2] as usize
                    ..self.key_data_offsets[self.key_data_offsets.len() - 1] as usize],
            )
        }
    }

    fn val_last(&self) -> Option<&[u8]> {
        if self.val_data_offsets.len() < 2 {
            None
        } else {
            Some(
                &self.val_data[self.val_data_offsets[self.val_data_offsets.len() - 2] as usize
                    ..self.val_data_offsets[self.val_data_offsets.len() - 1] as usize],
            )
        }
    }

    pub fn push(&mut self, element: (Vec<u8>, Vec<u8>, u64, i64)) {
        let (key, val, ts, diff) = element;

        self.key_idx_by_idx.push(self.key_data_offsets.len() as u32);
        if Some(&key[..]) != self.key_last() {
            self.key_data.extend(key);
            self.key_data_offsets.push(self.key_data.len() as u32);
        }

        self.val_idx_by_idx.push(self.val_data_offsets.len() as u32);
        if Some(&val[..]) != self.val_last() {
            self.val_data.extend(val);
            self.val_data_offsets.push(self.val_data.len() as u32);
        }

        self.timestamps.push(ts);
        self.diffs.push(diff);
    }

    pub fn done(self, lower: u64, upper: u64, since: u64) -> Vec<u8> {
        // WIP: We can compute the size ahead of time so this ends up being a
        // single segment message in a single pass.
        let mut message = ::capnp::message::Builder::new_default();
        {
            let mut batch = message.init_root::<capnpgen::batch_columnar::Builder>();
            batch.set_lower(lower);
            batch.set_upper(upper);
            batch.set_since(since);
            batch
                .reborrow()
                .init_key_data(self.key_data.len() as u32)
                .copy_from_slice(&self.key_data);
            batch
                .reborrow()
                .init_val_data(self.val_data.len() as u32)
                .copy_from_slice(&self.val_data);
            fill_capnp_list(
                &mut batch
                    .reborrow()
                    .init_key_data_offsets(self.key_data_offsets.len() as u32),
                &self.key_data_offsets,
            );
            fill_capnp_list(
                &mut batch
                    .reborrow()
                    .init_val_data_offsets(self.val_data_offsets.len() as u32),
                &self.val_data_offsets,
            );
            fill_capnp_list(
                &mut batch
                    .reborrow()
                    .init_timestamps(self.timestamps.len() as u32),
                &self.timestamps,
            );
            fill_capnp_list(
                &mut batch.reborrow().init_diffs(self.diffs.len() as u32),
                &self.diffs,
            );
            fill_capnp_list(
                &mut batch
                    .reborrow()
                    .init_key_idx_by_idx(self.key_idx_by_idx.len() as u32),
                &self.key_idx_by_idx,
            );
            fill_capnp_list(
                &mut batch
                    .reborrow()
                    .init_val_idx_by_idx(self.val_idx_by_idx.len() as u32),
                &self.val_idx_by_idx,
            );
        }
        // WIP: Assert that this message is canonically encoded.
        let mut buf = vec![];
        serialize::write_message(&mut buf, &message).expect("writes to Vec<u8> are infallable");
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use capnp::message::ReaderOptions;
    use capnp::serialize_packed;

    fn testdata() -> Vec<u8> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let batch = message.init_root::<capnpgen::batch::Builder>();
            let mut tuples = batch.init_tuples(4);
            let mut add_tuple = |idx, (key, val, ts, diff)| {
                let mut tuple = tuples.reborrow().get(idx);
                tuple.set_key(key);
                tuple.set_val(val);
                tuple.set_ts(ts);
                tuple.set_diff(diff);
            };
            add_tuple(0, (b"a", b"val-a", 3, 1));
            add_tuple(1, (b"b", b"val-b", 1, 1));
            add_tuple(2, (b"b", b"val-b", 4, 1));
            add_tuple(3, (b"c", b"val-c", 2, 1));
        }
        let mut buf = vec![];
        serialize_packed::write_message(&mut buf, &message)
            .expect("writes to Vec<u8> are infallable");
        buf
    }

    fn to_vec<'a, K: ?Sized, V: ?Sized, T: Clone, R: Clone, C: Cursor<K, V, T, R>>(
        cursor: &mut C,
        storage: &'a C::Storage,
    ) -> Vec<((&'a K, &'a V), Vec<(T, R)>)> {
        let mut out = Vec::new();
        cursor.rewind_keys(storage);
        cursor.rewind_vals(storage);
        while cursor.key_valid(storage) {
            while cursor.val_valid(storage) {
                let mut kv_out = Vec::new();
                cursor.map_times(storage, |ts, r| {
                    kv_out.push((ts.clone(), r.clone()));
                });
                out.push(((cursor.key(storage), cursor.val(storage)), kv_out));
                cursor.step_val(storage);
            }
            cursor.step_key(storage);
        }
        out
    }

    #[test]
    fn serde() -> Result<(), Box<dyn std::error::Error>> {
        let buf = testdata();
        let message_reader =
            serialize_packed::read_message(&mut buf.as_ref(), ReaderOptions::new())?;
        let b = message_reader.get_root::<capnpgen::batch::Reader>()?;
        let b = PBatch::from_reader(b)?;
        let mut c = b.cursor();
        assert_eq!(c.key(&b), &b"a"[..]);
        assert_eq!(c.val(&b), &b"val-a"[..]);
        c.seek_key(&b, &b"b"[..]);
        assert_eq!(c.key(&b), &b"b"[..]);
        assert_eq!(c.val(&b), &b"val-b"[..]);
        assert_eq!(
            to_vec(&mut c, &b),
            &[
                ((&b"a"[..], &b"val-a"[..]), vec![(3, 1)]),
                ((&b"b"[..], &b"val-b"[..]), vec![(1, 1), (4, 1)]),
                ((&b"c"[..], &b"val-c"[..]), vec![(2, 1)])
            ]
        );
        Ok(())
    }
}
