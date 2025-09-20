/*
 * Copyright (c) 2008–2025 Manuel J. Nieves (a.k.a. Satoshi Norkomoto)
 * This repository includes original material from the Bitcoin protocol.
 *
 * Redistribution requires this notice remain intact.
 * Derivative works must state derivative status.
 * Commercial use requires licensing.
 *
 * GPG Signed: B4EC 7343 AB0D BF24
 * Contact: Fordamboy1@gmail.com
 */
use bitcoin::{consensus::Decodable, Block};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use electrs::new_index::schema::bench::*;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("add_blocks", |b| {
        let block_bytes = bitcoin_test_data::blocks::mainnet_702861();
        let block = Block::consensus_decode(&mut &block_bytes[..]).unwrap();
        let data = Data::new(block);
        // TODO use iter_batched to avoid measuring cloning inputs

        b.iter(move || black_box(add_blocks(&data)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
