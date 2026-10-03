use criterion::{Criterion, criterion_group, criterion_main};
use distributed_kv::common::encoder::Encoder;
use std::hint::black_box; // once `encoder` is made pub

fn bench_encode(c: &mut Criterion) {
    let set_entry = distributed_kv::common::entry::Entry::set(1, b"key", b"value");
    c.bench_function("encode", |b| {
        b.iter(|| Encoder::encode(black_box(&set_entry)))
    });
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
