//! WIP

#![allow(unused_variables)]

use std::marker::PhantomData;

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;

use crate::trace::{Batch, BatchReader, Batcher, Builder, Cursor, Description, Merger};

/// WIP
pub struct ColBatch<T, R> {
    desc: Description<T>,
    _phantom: PhantomData<R>,
}

impl<T, R> Batch<Vec<u8>, [u8], Vec<u8>, [u8], T, R> for ColBatch<T, R> {
    type Batcher = ColBatcher<T, R>;

    type Builder = ColBuilder<T, R>;

    type Merger = ColMerger<T, R>;
}

/// WIP
pub struct ColBatchCursor<T, R> {
    _phantom: PhantomData<(T, R)>,
}

/// WIP
pub struct ColBatcher<T, R> {
    _phantom: PhantomData<(T, R)>,
}

/// WIP
pub struct ColBuilder<T, R> {
    _phantom: PhantomData<(T, R)>,
}

/// WIP
pub struct ColMerger<T, R> {
    _phantom: PhantomData<(T, R)>,
}

impl<T, R> BatchReader<[u8], [u8], T, R> for ColBatch<T, R> {
    type Cursor = ColBatchCursor<T, R>;

    fn cursor(&self) -> Self::Cursor {
        ColBatchCursor {
            _phantom: PhantomData,
        }
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn description(&self) -> &Description<T> {
        &self.desc
    }
}

impl<T, R> Cursor<[u8], [u8], T, R> for ColBatchCursor<T, R> {
    type Storage = ColBatch<T, R>;

    fn key_valid(&self, storage: &Self::Storage) -> bool {
        todo!()
    }

    fn val_valid(&self, storage: &Self::Storage) -> bool {
        todo!()
    }

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a [u8] {
        todo!()
    }

    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a [u8] {
        todo!()
    }

    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, logic: L) {
        todo!()
    }

    fn step_key(&mut self, storage: &Self::Storage) {
        todo!()
    }

    fn seek_key(&mut self, storage: &Self::Storage, key: &[u8]) {
        todo!()
    }

    fn step_val(&mut self, storage: &Self::Storage) {
        todo!()
    }

    fn seek_val(&mut self, storage: &Self::Storage, val: &[u8]) {
        todo!()
    }

    fn rewind_keys(&mut self, storage: &Self::Storage) {
        todo!()
    }

    fn rewind_vals(&mut self, storage: &Self::Storage) {
        todo!()
    }
}

impl<T, R> Batcher<Vec<u8>, [u8], Vec<u8>, [u8], T, R, ColBatch<T, R>> for ColBatcher<T, R> {
    fn new() -> Self {
        todo!()
    }

    fn push_batch(&mut self, batch: &mut Vec<((Vec<u8>, Vec<u8>), T, R)>) {
        todo!()
    }

    fn seal(&mut self, upper: Antichain<T>) -> ColBatch<T, R> {
        todo!()
    }

    fn frontier(&mut self) -> AntichainRef<T> {
        todo!()
    }
}

impl<T, R> Builder<Vec<u8>, [u8], Vec<u8>, [u8], T, R, ColBatch<T, R>> for ColBuilder<T, R> {
    fn new() -> Self {
        todo!()
    }

    fn with_capacity(cap: usize) -> Self {
        todo!()
    }

    fn push(&mut self, element: (Vec<u8>, Vec<u8>, T, R)) {
        todo!()
    }

    fn done(self, lower: Antichain<T>, upper: Antichain<T>, since: Antichain<T>) -> ColBatch<T, R> {
        todo!()
    }
}

impl<T, R> Merger<Vec<u8>, [u8], Vec<u8>, [u8], T, R, ColBatch<T, R>> for ColMerger<T, R> {
    fn new(
        source1: &ColBatch<T, R>,
        source2: &ColBatch<T, R>,
        compaction_frontier: Option<AntichainRef<T>>,
    ) -> Self {
        todo!()
    }

    fn work(&mut self, source1: &ColBatch<T, R>, source2: &ColBatch<T, R>, fuel: &mut isize) {
        todo!()
    }

    fn done(self) -> ColBatch<T, R> {
        todo!()
    }
}
