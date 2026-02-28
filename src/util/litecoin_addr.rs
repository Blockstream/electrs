use bitcoin::hashes::{sha256d, Hash};
use bitcoin::Script;

use crate::chain::Network;

/// Version bytes for Litecoin base58 addresses
const LITECOIN_P2PKH_VERSION: u8 = 0x30; // 'L' prefix
const LITECOIN_P2SH_VERSION: u8 = 0x32; // 'M' prefix
const LITECOIN_TESTNET_P2PKH_VERSION: u8 = 0x6f; // 'm' or 'n' prefix
const LITECOIN_TESTNET_P2SH_VERSION: u8 = 0xc4; // '2' prefix

fn network_params(network: Network) -> (u8, u8, &'static str) {
    match network {
        Network::Litecoin => (LITECOIN_P2PKH_VERSION, LITECOIN_P2SH_VERSION, "ltc"),
        Network::LitecoinTestnet => (
            LITECOIN_TESTNET_P2PKH_VERSION,
            LITECOIN_TESTNET_P2SH_VERSION,
            "tltc",
        ),
        Network::LitecoinRegtest => (
            LITECOIN_TESTNET_P2PKH_VERSION,
            LITECOIN_TESTNET_P2SH_VERSION,
            "rltc",
        ),
    }
}

/// Convert a scriptPubKey to a Litecoin address string
pub fn script_to_litecoin_address(script: &Script, network: Network) -> Option<String> {
    let (p2pkh_ver, p2sh_ver, hrp) = network_params(network);
    let bytes = script.as_bytes();

    if script.is_p2pkh() && bytes.len() == 25 {
        Some(base58check_encode(p2pkh_ver, &bytes[3..23]))
    } else if script.is_p2sh() && bytes.len() == 23 {
        Some(base58check_encode(p2sh_ver, &bytes[2..22]))
    } else if script.is_p2wpkh() || script.is_p2wsh() || script.is_p2tr() {
        // Segwit: witness version byte + push length + witness program
        if bytes.len() < 2 {
            return None;
        }
        let witness_version = if bytes[0] == 0x00 {
            0u8
        } else if (0x51..=0x60).contains(&bytes[0]) {
            bytes[0] - 0x50
        } else {
            return None;
        };
        let program = &bytes[2..];
        bech32_segwit_encode(hrp, witness_version, program)
    } else {
        None
    }
}

/// Parse a Litecoin address string and return the scriptPubKey bytes
pub fn parse_litecoin_address(addr: &str, network: Network) -> Option<Vec<u8>> {
    let (p2pkh_ver, p2sh_ver, hrp) = network_params(network);

    // Try bech32/bech32m first
    if let Some(script) = bech32_segwit_decode(addr, hrp) {
        return Some(script);
    }

    // Try base58check
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

/// Validate that a Litecoin address is valid for the given network
pub fn validate_litecoin_address(addr: &str, network: Network) -> bool {
    parse_litecoin_address(addr, network).is_some()
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

// --- Bech32/Bech32m ---

const BECH32_CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BECH32_CONST: u32 = 1;
const BECH32M_CONST: u32 = 0x2bc830a3;

fn bech32_polymod(values: &[u8]) -> u32 {
    let mut chk: u32 = 1;
    for &v in values {
        let top = chk >> 25;
        chk = ((chk & 0x1ffffff) << 5) ^ (v as u32);
        if top & 1 != 0 {
            chk ^= 0x3b6a57b2;
        }
        if top & 2 != 0 {
            chk ^= 0x26508e6d;
        }
        if top & 4 != 0 {
            chk ^= 0x1ea119fa;
        }
        if top & 8 != 0 {
            chk ^= 0x3d4233dd;
        }
        if top & 16 != 0 {
            chk ^= 0x2a1462b3;
        }
    }
    chk
}

fn bech32_hrp_expand(hrp: &str) -> Vec<u8> {
    let mut ret = Vec::with_capacity(hrp.len() * 2 + 1);
    for c in hrp.chars() {
        ret.push((c as u8) >> 5);
    }
    ret.push(0);
    for c in hrp.chars() {
        ret.push((c as u8) & 31);
    }
    ret
}

fn bech32_create_checksum(hrp: &str, data: &[u8], spec: u32) -> Vec<u8> {
    let mut values = bech32_hrp_expand(hrp);
    values.extend_from_slice(data);
    values.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    let polymod = bech32_polymod(&values) ^ spec;
    (0..6)
        .map(|i| ((polymod >> (5 * (5 - i))) & 31) as u8)
        .collect()
}

fn bech32_verify_checksum(hrp: &str, data: &[u8]) -> Option<u32> {
    let mut values = bech32_hrp_expand(hrp);
    values.extend_from_slice(data);
    let polymod = bech32_polymod(&values);
    if polymod == BECH32_CONST {
        Some(BECH32_CONST)
    } else if polymod == BECH32M_CONST {
        Some(BECH32M_CONST)
    } else {
        None
    }
}

fn bech32_segwit_encode(hrp: &str, witness_version: u8, witness_program: &[u8]) -> Option<String> {
    // Convert 8-bit program to 5-bit groups
    let mut data5 = vec![witness_version];
    data5.extend(convert_bits(witness_program, 8, 5, true)?);

    let spec = if witness_version == 0 {
        BECH32_CONST
    } else {
        BECH32M_CONST
    };
    let checksum = bech32_create_checksum(hrp, &data5, spec);

    let mut result = String::from(hrp);
    result.push('1');
    for &d in data5.iter().chain(checksum.iter()) {
        result.push(BECH32_CHARSET[d as usize] as char);
    }
    Some(result)
}

fn bech32_segwit_decode(addr: &str, expected_hrp: &str) -> Option<Vec<u8>> {
    let addr_lower = addr.to_lowercase();
    let pos = addr_lower.rfind('1')?;
    if pos == 0 || pos + 7 > addr_lower.len() {
        return None;
    }

    let hrp = &addr_lower[..pos];
    if hrp != expected_hrp {
        return None;
    }

    let data_part = &addr_lower[pos + 1..];
    let mut data: Vec<u8> = Vec::with_capacity(data_part.len());
    for c in data_part.chars() {
        let pos = BECH32_CHARSET.iter().position(|&ch| ch == c as u8)?;
        data.push(pos as u8);
    }

    let spec = bech32_verify_checksum(hrp, &data)?;

    // Remove checksum (last 6 chars)
    let data = &data[..data.len() - 6];
    if data.is_empty() {
        return None;
    }

    let witness_version = data[0];
    if witness_version > 16 {
        return None;
    }

    // Verify correct bech32 variant
    if witness_version == 0 && spec != BECH32_CONST {
        return None;
    }
    if witness_version != 0 && spec != BECH32M_CONST {
        return None;
    }

    let program = convert_bits(&data[1..], 5, 8, false)?;

    // Validate program length per BIP141
    if program.len() < 2 || program.len() > 40 {
        return None;
    }
    if witness_version == 0 && program.len() != 20 && program.len() != 32 {
        return None;
    }

    // Build scriptPubKey
    let opcode = if witness_version == 0 {
        0x00u8
    } else {
        0x50 + witness_version
    };
    let mut script = vec![opcode, program.len() as u8];
    script.extend_from_slice(&program);
    Some(script)
}

fn convert_bits(data: &[u8], from_bits: u32, to_bits: u32, pad: bool) -> Option<Vec<u8>> {
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let mut ret = Vec::new();
    let maxv = (1u32 << to_bits) - 1;

    for &value in data {
        if (value as u32) >> from_bits != 0 {
            return None;
        }
        acc = (acc << from_bits) | value as u32;
        bits += from_bits;
        while bits >= to_bits {
            bits -= to_bits;
            ret.push(((acc >> bits) & maxv) as u8);
        }
    }

    if pad {
        if bits > 0 {
            ret.push(((acc << (to_bits - bits)) & maxv) as u8);
        }
    } else if bits >= from_bits || ((acc << (to_bits - bits)) & maxv) != 0 {
        return None;
    }

    Some(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base58check_roundtrip() {
        let hash = [0u8; 20];
        let encoded = base58check_encode(LITECOIN_P2PKH_VERSION, &hash);
        let (version, decoded) = base58check_decode(&encoded).unwrap();
        assert_eq!(version, LITECOIN_P2PKH_VERSION);
        assert_eq!(decoded, hash);
    }

    #[test]
    fn test_litecoin_p2pkh_address_prefix() {
        let hash = [1u8; 20];
        let addr = base58check_encode(LITECOIN_P2PKH_VERSION, &hash);
        assert!(
            addr.starts_with('L') || addr.starts_with('K'),
            "P2PKH address should start with L or K, got: {}",
            addr
        );
    }

    #[test]
    fn test_litecoin_p2sh_address_prefix() {
        let hash = [1u8; 20];
        let addr = base58check_encode(LITECOIN_P2SH_VERSION, &hash);
        assert!(
            addr.starts_with('M'),
            "P2SH address should start with M, got: {}",
            addr
        );
    }

    #[test]
    fn test_bech32_encode_decode_roundtrip() {
        let program = [0u8; 20]; // P2WPKH-like
        let encoded = bech32_segwit_encode("ltc", 0, &program).unwrap();
        assert!(encoded.starts_with("ltc1"));

        let script = bech32_segwit_decode(&encoded, "ltc").unwrap();
        // script should be: OP_0 <20> <20 zero bytes>
        assert_eq!(script[0], 0x00); // OP_0
        assert_eq!(script[1], 20); // push 20 bytes
        assert_eq!(&script[2..], &program);
    }

    #[test]
    fn test_bech32m_encode_decode_roundtrip() {
        let program = [1u8; 32]; // P2TR-like (witness v1, 32 bytes)
        let encoded = bech32_segwit_encode("ltc", 1, &program).unwrap();
        assert!(encoded.starts_with("ltc1p"));

        let script = bech32_segwit_decode(&encoded, "ltc").unwrap();
        assert_eq!(script[0], 0x51); // OP_1
        assert_eq!(script[1], 32);
        assert_eq!(&script[2..], &program);
    }

    #[test]
    fn test_bech32_wrong_hrp_rejected() {
        let program = [0u8; 20];
        let encoded = bech32_segwit_encode("ltc", 0, &program).unwrap();
        assert!(bech32_segwit_decode(&encoded, "tltc").is_none());
    }

    #[test]
    fn test_parse_and_validate_roundtrip() {
        // Create a P2PKH scriptPubKey
        let mut p2pkh_script = vec![0x76, 0xa9, 0x14];
        p2pkh_script.extend_from_slice(&[1u8; 20]);
        p2pkh_script.extend_from_slice(&[0x88, 0xac]);

        let script = bitcoin::ScriptBuf::from(p2pkh_script.clone());
        let addr = script_to_litecoin_address(&script, Network::Litecoin).unwrap();
        let decoded_script = parse_litecoin_address(&addr, Network::Litecoin).unwrap();
        assert_eq!(decoded_script, p2pkh_script);
    }
}
