//! Persistent implementations of trace datastructures.

use std::cmp;

use differential_dataflow::trace::{BatchReader, Cursor, Description};
use timely::progress::frontier::Antichain;

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
    // pub fn from_bytes(buf: Vec<u8>) -> Result<Self, Box<dyn std::error::Error>> {
    //     let buf = buf.as_ref();
    //     let message_reader = serialize_packed::read_message(&mut buf, ReaderOptions::new())?;
    //     let b = message_reader.get_root::<capnpgen::batch::Reader>()?;
    //     Batch::from_reader(b)
    // }

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
            // Key not found. No-op.
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
