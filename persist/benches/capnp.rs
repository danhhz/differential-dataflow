//! Benchmarks for capnp based batch persistence.

use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use criterion::{criterion_group, criterion_main, Criterion};

use differential_dataflow::trace::{BatchReader, Cursor};
use persist::{capnpgen, PBatch};

fn gen_batch(tuples: u32) -> Vec<u8> {
    const TUPLES_PER_VAL: u32 = 2;
    const VALS_PER_KEY: u32 = 2;
    let keys = tuples / TUPLES_PER_VAL / VALS_PER_KEY;
    let key_len = (keys as f64).log(16.0) as u32 + 1;

    let mut message = ::capnp::message::Builder::new_default();
    {
        let batch = message.init_root::<capnpgen::batch::Builder>();
        let mut t = batch.init_tuples(tuples as u32);
        for tuple_idx in 0..tuples {
            let key_idx = tuple_idx / (TUPLES_PER_VAL * VALS_PER_KEY);
            let key = format!("{:0width$x}", key_idx, width = key_len as usize);
            let val_idx = (tuple_idx % (TUPLES_PER_VAL * VALS_PER_KEY)) / VALS_PER_KEY;
            let val = format!("{}-{:x}", key, val_idx);
            let mut tuple = t.reborrow().get(tuple_idx);
            tuple.set_key(key.as_bytes());
            tuple.set_val(val.as_bytes());
            tuple.set_ts(tuple_idx.into());
            tuple.set_diff(1);
        }
    }
    let mut buf = vec![];
    serialize_packed::write_message(&mut buf, &message).expect("writes to Vec<u8> are infallable");
    buf
}

fn capnp_benchmark(c: &mut Criterion) {
    let batch = gen_batch(1000000);

    c.bench_function("decode", |b| {
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
    c.bench_function("get_key", |b| {
        b.iter(|| {
            let c = batch.cursor();
            assert!(c.get_key(&batch).is_some());
        })
    });
    c.bench_function("seek_key", |b| {
        b.iter(|| {
            let mut c = batch.cursor();
            c.seek_key(&batch, &b"b"[..]);
            assert!(c.get_key(&batch).is_some());
        })
    });
}

criterion_group!(benches, capnp_benchmark);
criterion_main!(benches);
