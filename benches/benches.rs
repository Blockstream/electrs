use bitcoin::{consensus::Decodable, Block};
use criterion::{criterion_group, criterion_main, Criterion};
use electrs::new_index::schema::bench::*;
use std::hint::black_box;

fn witness_proof_benchmark(c: &mut Criterion) {
    use bitcoin::hashes::{sha256d, Hash};
    use electrs::util::electrum_merkle::create_merkle_branch_and_root;

    // CPU cost of a witness-merkle proof over a real mainnet block
    // (block 702861: ~1000 txs): wtxid computation for every tx (the coinbase
    // leaf is zeroed per BIP-141) plus branch construction. The DB-lookup cost
    // of fetching the block's transactions is deployment-dependent and equals
    // the existing GET /block/:hash/raw path (same lookup_txns batch).
    let block_bytes = bitcoin_test_data::blocks::mainnet_702861();
    let block = Block::consensus_decode(&mut &block_bytes[..]).unwrap();

    c.bench_function("witness_proof_wtxids_block_702861", |b| {
        b.iter(|| {
            let wtxids: Vec<sha256d::Hash> = block
                .txdata
                .iter()
                .enumerate()
                .map(|(i, tx)| {
                    if i == 0 {
                        sha256d::Hash::all_zeros()
                    } else {
                        tx.compute_wtxid().to_raw_hash()
                    }
                })
                .collect();
            black_box(wtxids)
        })
    });

    c.bench_function("witness_proof_branch_block_702861", |b| {
        let wtxids: Vec<sha256d::Hash> = block
            .txdata
            .iter()
            .enumerate()
            .map(|(i, tx)| {
                if i == 0 {
                    sha256d::Hash::all_zeros()
                } else {
                    tx.compute_wtxid().to_raw_hash()
                }
            })
            .collect();
        let mid = wtxids.len() / 2;
        b.iter(|| black_box(create_merkle_branch_and_root(wtxids.clone(), mid)))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("add_blocks", |b| {
        let block_bytes = bitcoin_test_data::blocks::mainnet_702861();
        let block = Block::consensus_decode(&mut &block_bytes[..]).unwrap();
        let data = Data::new(block);
        // TODO use iter_batched to avoid measuring cloning inputs

        b.iter(move || black_box(add_blocks(&data)))
    });
}

criterion_group!(benches, criterion_benchmark, witness_proof_benchmark);
criterion_main!(benches);
