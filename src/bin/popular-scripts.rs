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
extern crate electrs;

use bitcoin::hex::DisplayHex;
use electrs::{
    config::Config,
    new_index::{Store, TxHistoryKey},
    util::bincode,
};

fn main() {
    let config = Config::from_args();
    let store = Store::open(&config.db_path.join("newindex"), &config);

    let mut iter = store.history_db().raw_iterator();
    iter.seek(b"H");

    let mut curr_scripthash = [0u8; 32];
    let mut total_entries = 0;

    while iter.valid() {
        let key = iter.key().unwrap();

        if !key.starts_with(b"H") {
            break;
        }

        let entry: TxHistoryKey =
            bincode::deserialize_big(&key).expect("failed to deserialize TxHistoryKey");

        if curr_scripthash != entry.hash {
            if total_entries > 100 {
                println!(
                    "{} {}",
                    curr_scripthash.to_lower_hex_string(),
                    total_entries
                );
            }

            curr_scripthash = entry.hash;
            total_entries = 0;
        }

        total_entries += 1;

        iter.next();
    }

    if total_entries >= 4000 {
        println!(
            "scripthash,{},{}",
            curr_scripthash.to_lower_hex_string(),
            total_entries
        );
    }
}
