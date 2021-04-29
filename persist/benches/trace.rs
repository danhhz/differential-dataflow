//! Benchmarks for capnp based batch persistence.

use capnp::message::{HeapAllocator, ReaderOptions};
use capnp::serialize_packed;
use criterion::{criterion_group, criterion_main, Criterion};

use differential_dataflow::trace::implementations::ord::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::{BatchReader, Builder, Cursor};
use persist::trace::{capnpgen, PBatch};
use timely::progress::Antichain;

fn gen_tuples<F: FnMut(u32, String, String, u64, i64)>(tuples: u32, mut set_tuple: F) {
    const TUPLES_PER_VAL: u32 = 2;
    const VALS_PER_KEY: u32 = 2;
    let keys = tuples / TUPLES_PER_VAL / VALS_PER_KEY;
    let key_len = (keys as f64).log(16.0) as u32 + 1;

    for tuple_idx in 0..tuples {
        let key_idx = tuple_idx / (TUPLES_PER_VAL * VALS_PER_KEY);
        let key = format!("{:0width$x}", key_idx, width = key_len as usize);
        let val_idx = (tuple_idx % (TUPLES_PER_VAL * VALS_PER_KEY)) / VALS_PER_KEY;
        let val = format!("{}-{:x}", key, val_idx);
        set_tuple(tuple_idx, key, val, tuple_idx.into(), 1)
    }
}

fn gen_capnp(tuples: u32) -> Vec<u8> {
    let mut message = ::capnp::message::Builder::new_default();
    let mut batch = message.init_root::<capnpgen::batch::Builder>();
    let mut t = batch.reborrow().init_tuples(tuples as u32);
    gen_tuples(
        tuples,
        |idx: u32, key: String, val: String, ts: u64, diff: i64| {
            let mut tuple = t.reborrow().get(idx);
            tuple.set_key(key.as_bytes());
            tuple.set_val(val.as_bytes());
            tuple.set_ts(ts);
            tuple.set_diff(diff);
        },
    );
    // Canonicalize on write so we don't need the far pointer jumps on read.
    let batch = batch.into_reader();
    let mut canonical = ::capnp::message::Builder::new(
        HeapAllocator::new().first_segment_words(batch.total_size().unwrap().word_count as u32),
    );
    canonical
        .set_root_canonical(batch)
        .expect("guaranteed valid message");
    let mut buf = vec![];
    serialize_packed::write_message(&mut buf, &canonical)
        .expect("writes to Vec<u8> are infallable");
    buf
}

fn gen_mem(tuples: u32) -> OrdValBatch<String, String, u64, i64, u32> {
    let mut batch = OrdValBuilder::with_capacity(tuples as usize);
    gen_tuples(
        tuples,
        |_idx: u32, key: String, val: String, ts: u64, diff: i64| {
            batch.push((key, val, ts, diff));
        },
    );
    batch.done(
        Antichain::from_elem(0),
        Antichain::from_elem(0),
        Antichain::from_elem(0),
    )
}

fn capnp_benchmark(c: &mut Criterion) {
    let batch = gen_capnp(1000000);

    c.bench_function("capnp_decode", |b| {
        b.iter(|| {
            let message_reader =
                serialize_packed::read_message(&mut &batch[..], ReaderOptions::new()).unwrap();
            let b = message_reader
                .get_root::<capnpgen::batch::Reader>()
                .unwrap();
            let _ = PBatch::from_reader(b).unwrap();
        })
    });

    let message_reader = serialize_packed::read_message(
        &mut &batch[..],
        *ReaderOptions::new().traversal_limit_in_words(None), // Well this is unfortunate
    )
    .unwrap();
    let batch = message_reader
        .get_root::<capnpgen::batch::Reader>()
        .unwrap();
    let batch = PBatch::from_reader(batch).unwrap();
    c.bench_function("capnp_get_key", |b| {
        b.iter(|| {
            let c = batch.cursor();
            assert!(c.get_key(&batch).is_some());
        })
    });
    c.bench_function("capnp_seek_key", |b| {
        b.iter(|| {
            let mut c = batch.cursor();
            c.seek_key(&batch, &b"0000b"[..]);
            assert!(c.get_key(&batch).is_some());
        })
    });
}

fn mem_benchmark(c: &mut Criterion) {
    let batch = gen_mem(1000000);

    c.bench_function("mem_get_key", |b| {
        b.iter(|| {
            let c = batch.cursor();
            assert!(c.get_key(&batch).is_some());
        })
    });
    c.bench_function("mem_seek_key", |b| {
        b.iter(|| {
            let mut c = batch.cursor();
            c.seek_key(&batch, &"0000b".to_string());
            assert!(c.get_key(&batch).is_some());
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = capnp_benchmark, mem_benchmark
}
criterion_main!(benches);
