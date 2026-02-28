use bitcoin::blockdata::block::Header as BlockHeader;
use bitcoin::consensus::Decodable;
use bitcoin::hashes::{sha256d, Hash};
use bitcoin::{Block, Script, Transaction, VarInt};
use std::io::Cursor;

use crate::chain::Network;

// --- Address encoding ---

/// Version bytes for Dogecoin base58 addresses
const DOGECOIN_P2PKH_VERSION: u8 = 0x1e; // 'D' prefix
const DOGECOIN_P2SH_VERSION: u8 = 0x16; // '9' or 'A' prefix
const DOGECOIN_TESTNET_P2PKH_VERSION: u8 = 0x71; // 'n' prefix
const DOGECOIN_TESTNET_P2SH_VERSION: u8 = 0xc4; // '2' prefix

fn network_params(network: Network) -> (u8, u8) {
    match network {
        Network::Dogecoin => (DOGECOIN_P2PKH_VERSION, DOGECOIN_P2SH_VERSION),
        Network::DogecoinTestnet => (
            DOGECOIN_TESTNET_P2PKH_VERSION,
            DOGECOIN_TESTNET_P2SH_VERSION,
        ),
        Network::DogecoinRegtest => (
            DOGECOIN_TESTNET_P2PKH_VERSION,
            DOGECOIN_TESTNET_P2SH_VERSION,
        ),
    }
}

/// Convert a scriptPubKey to a Dogecoin address string.
/// Dogecoin does not support SegWit, so only P2PKH and P2SH are handled.
pub fn script_to_dogecoin_address(script: &Script, network: Network) -> Option<String> {
    let (p2pkh_ver, p2sh_ver) = network_params(network);
    let bytes = script.as_bytes();

    if script.is_p2pkh() && bytes.len() == 25 {
        Some(base58check_encode(p2pkh_ver, &bytes[3..23]))
    } else if script.is_p2sh() && bytes.len() == 23 {
        Some(base58check_encode(p2sh_ver, &bytes[2..22]))
    } else {
        None
    }
}

/// Parse a Dogecoin address string and return the scriptPubKey bytes
pub fn parse_dogecoin_address(addr: &str, network: Network) -> Option<Vec<u8>> {
    let (p2pkh_ver, p2sh_ver) = network_params(network);

    let (version, hash) = base58check_decode(addr)?;
    if version == p2pkh_ver && hash.len() == 20 {
        // P2PKH: OP_DUP OP_HASH160 <20> <hash> OP_EQUALVERIFY OP_CHECKSIG
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&hash);
        script.extend_from_slice(&[0x88, 0xac]);
        Some(script)
    } else if version == p2sh_ver && hash.len() == 20 {
        // P2SH: OP_HASH160 <20> <hash> OP_EQUAL
        let mut script = vec![0xa9, 0x14];
        script.extend_from_slice(&hash);
        script.push(0x87);
        Some(script)
    } else {
        None
    }
}

// --- Base58Check ---

fn base58check_encode(version: u8, payload: &[u8]) -> String {
    let mut data = Vec::with_capacity(1 + payload.len() + 4);
    data.push(version);
    data.extend_from_slice(payload);
    let checksum = sha256d::Hash::hash(&data);
    data.extend_from_slice(&checksum[..4]);
    base58_encode(&data)
}

fn base58check_decode(addr: &str) -> Option<(u8, Vec<u8>)> {
    let data = base58_decode(addr)?;
    if data.len() < 5 {
        return None;
    }
    let (payload, checksum) = data.split_at(data.len() - 4);
    let hash = sha256d::Hash::hash(payload);
    if &hash[..4] != checksum {
        return None;
    }
    Some((payload[0], payload[1..].to_vec()))
}

const BASE58_ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn base58_encode(data: &[u8]) -> String {
    let leading_zeros = data.iter().take_while(|&&b| b == 0).count();
    let mut num = data.to_vec();
    let mut result = Vec::new();

    while !num.is_empty() {
        let mut remainder = 0u32;
        let mut new_num = Vec::new();
        for &byte in &num {
            let acc = (remainder << 8) | byte as u32;
            let digit = acc / 58;
            remainder = acc % 58;
            if !new_num.is_empty() || digit > 0 {
                new_num.push(digit as u8);
            }
        }
        result.push(BASE58_ALPHABET[remainder as usize]);
        num = new_num;
    }

    for _ in 0..leading_zeros {
        result.push(b'1');
    }
    result.reverse();
    String::from_utf8(result).unwrap()
}

fn base58_decode(s: &str) -> Option<Vec<u8>> {
    let leading_ones = s.chars().take_while(|&c| c == '1').count();
    let mut result: Vec<u8> = Vec::new();

    for ch in s.chars() {
        let pos = BASE58_ALPHABET.iter().position(|&c| c == ch as u8)? as u32;
        let mut carry = pos;
        for byte in result.iter_mut().rev() {
            let acc = (*byte as u32) * 58 + carry;
            *byte = (acc & 0xff) as u8;
            carry = acc >> 8;
        }
        while carry > 0 {
            result.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }

    let mut final_result = vec![0u8; leading_ones];
    final_result.extend_from_slice(&result);
    Some(final_result)
}

// --- AuxPoW Block Deserialization ---

/// The AuxPoW version flag in the block header version field.
/// If `(version >> 8) & 0xFF != 0` (specifically bit 8 set), AuxPoW data follows the header.
const BLOCK_VERSION_AUXPOW: i32 = 1 << 8;

/// Deserialize a Dogecoin block, handling AuxPoW data if present.
///
/// Dogecoin uses Auxiliary Proof of Work (merged mining with Litecoin).
/// AuxPoW blocks have extra data between the 80-byte header and the transaction list:
///   [80-byte header][AuxPoW data][varint tx_count][transactions...]
///
/// Standard Bitcoin blocks are:
///   [80-byte header][varint tx_count][transactions...]
///
/// This function reads the header, skips AuxPoW data if present,
/// then reads transactions, constructing a standard `bitcoin::Block`.
pub fn deserialize_dogecoin_block(data: &[u8]) -> Result<Block, bitcoin::consensus::encode::Error> {
    let mut cursor = Cursor::new(data);

    // Read the 80-byte block header
    let header = BlockHeader::consensus_decode(&mut cursor)?;

    // Check for AuxPoW flag in the version field
    if header.version.to_consensus() & BLOCK_VERSION_AUXPOW != 0 {
        skip_auxpow(&mut cursor)?;
    }

    // Read transactions
    let txdata = Vec::<Transaction>::consensus_decode(&mut cursor)?;

    Ok(Block { header, txdata })
}

/// Skip over AuxPoW data in the byte stream.
///
/// AuxPoW structure:
/// 1. Coinbase transaction (parent chain) - variable length
/// 2. Block hash (32 bytes)
/// 3. Coinbase merkle branch: varint count + count*32 byte hashes + i32 side_mask
/// 4. Blockchain merkle branch: varint count + count*32 byte hashes + i32 side_mask
/// 5. Parent block header (80 bytes)
fn skip_auxpow(cursor: &mut Cursor<&[u8]>) -> Result<(), bitcoin::consensus::encode::Error> {
    // 1. Skip coinbase transaction
    Transaction::consensus_decode(cursor)?;

    // 2. Skip block hash (32 bytes)
    <[u8; 32]>::consensus_decode(cursor)?;

    // 3. Skip coinbase merkle branch
    skip_merkle_branch(cursor)?;

    // 4. Skip blockchain merkle branch
    skip_merkle_branch(cursor)?;

    // 5. Skip parent block header (80 bytes)
    BlockHeader::consensus_decode(cursor)?;

    Ok(())
}

/// Skip a merkle branch: varint count of hashes, then count * 32-byte hashes, then i32 side_mask.
fn skip_merkle_branch(cursor: &mut Cursor<&[u8]>) -> Result<(), bitcoin::consensus::encode::Error> {
    let count = VarInt::consensus_decode(cursor)?.0;
    for _ in 0..count {
        <[u8; 32]>::consensus_decode(cursor)?;
    }
    // side_mask (i32, 4 bytes)
    i32::consensus_decode(cursor)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base58check_roundtrip() {
        let hash = [0u8; 20];
        let encoded = base58check_encode(DOGECOIN_P2PKH_VERSION, &hash);
        let (version, decoded) = base58check_decode(&encoded).unwrap();
        assert_eq!(version, DOGECOIN_P2PKH_VERSION);
        assert_eq!(decoded, hash);
    }

    #[test]
    fn test_dogecoin_p2pkh_address_prefix() {
        let hash = [1u8; 20];
        let addr = base58check_encode(DOGECOIN_P2PKH_VERSION, &hash);
        assert!(
            addr.starts_with('D'),
            "P2PKH address should start with D, got: {}",
            addr
        );
    }

    #[test]
    fn test_dogecoin_p2sh_address_prefix() {
        let hash = [1u8; 20];
        let addr = base58check_encode(DOGECOIN_P2SH_VERSION, &hash);
        assert!(
            addr.starts_with('9') || addr.starts_with('A'),
            "P2SH address should start with 9 or A, got: {}",
            addr
        );
    }

    #[test]
    fn test_parse_and_validate_roundtrip() {
        // Create a P2PKH scriptPubKey
        let mut p2pkh_script = vec![0x76, 0xa9, 0x14];
        p2pkh_script.extend_from_slice(&[1u8; 20]);
        p2pkh_script.extend_from_slice(&[0x88, 0xac]);

        let script = bitcoin::ScriptBuf::from(p2pkh_script.clone());
        let addr = script_to_dogecoin_address(&script, Network::Dogecoin).unwrap();
        let decoded_script = parse_dogecoin_address(&addr, Network::Dogecoin).unwrap();
        assert_eq!(decoded_script, p2pkh_script);
    }

    #[test]
    fn test_wrong_network_rejected() {
        let hash = [1u8; 20];
        let addr = base58check_encode(DOGECOIN_P2PKH_VERSION, &hash);
        // Should not parse as testnet
        assert!(parse_dogecoin_address(&addr, Network::DogecoinTestnet).is_none());
    }
}
