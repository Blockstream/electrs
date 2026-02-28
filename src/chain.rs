#[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
// use regular Bitcoin data structures
pub use bitcoin::{
    address, blockdata::block::Header as BlockHeader, blockdata::script, consensus::deserialize,
    hash_types::TxMerkleNode, Address, Block, BlockHash, OutPoint, ScriptBuf as Script, Sequence,
    Transaction, TxIn, TxOut, Txid,
};

// Litecoin uses the same Bitcoin data structures (same serialization format)
#[cfg(feature = "litecoin")]
pub use bitcoin::{
    address, blockdata::block::Header as BlockHeader, blockdata::script, consensus::deserialize,
    hash_types::TxMerkleNode, Address, Block, BlockHash, OutPoint, ScriptBuf as Script, Sequence,
    Transaction, TxIn, TxOut, Txid,
};

// Dogecoin uses the same Bitcoin data structures, but blocks need AuxPoW-aware deserialization
#[cfg(feature = "dogecoin")]
pub use bitcoin::{
    address, blockdata::block::Header as BlockHeader, blockdata::script, consensus::deserialize,
    hash_types::TxMerkleNode, Address, Block, BlockHash, OutPoint, ScriptBuf as Script, Sequence,
    Transaction, TxIn, TxOut, Txid,
};

#[cfg(feature = "liquid")]
pub use {
    crate::elements::asset,
    elements::{
        address, confidential, encode::deserialize, script, Address, AssetId, Block, BlockHash,
        BlockHeader, OutPoint, Script, Sequence, Transaction, TxIn, TxMerkleNode, TxOut, Txid,
    },
};

use bitcoin::blockdata::constants::genesis_block;
pub use bitcoin::network::Network as BNetwork;

#[cfg(not(feature = "liquid"))]
pub type Value = u64;
#[cfg(feature = "liquid")]
pub use confidential::Value;

#[derive(Debug, Copy, Clone, PartialEq, Hash, Serialize, Ord, PartialOrd, Eq)]
pub enum Network {
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    Bitcoin,
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    Testnet,
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    Testnet4,
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    Regtest,
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    Signet,

    #[cfg(feature = "liquid")]
    Liquid,
    #[cfg(feature = "liquid")]
    LiquidTestnet,
    #[cfg(feature = "liquid")]
    LiquidRegtest,

    #[cfg(feature = "litecoin")]
    Litecoin,
    #[cfg(feature = "litecoin")]
    LitecoinTestnet,
    #[cfg(feature = "litecoin")]
    LitecoinRegtest,

    #[cfg(feature = "dogecoin")]
    Dogecoin,
    #[cfg(feature = "dogecoin")]
    DogecoinTestnet,
    #[cfg(feature = "dogecoin")]
    DogecoinRegtest,
}

impl Network {
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    pub fn magic(self) -> u32 {
        u32::from_le_bytes(BNetwork::from(self).magic().to_bytes())
    }

    #[cfg(feature = "liquid")]
    pub fn magic(self) -> u32 {
        match self {
            Network::Liquid | Network::LiquidRegtest => 0xDAB5_BFFA,
            Network::LiquidTestnet => 0x62DD_0E41,
        }
    }

    #[cfg(feature = "litecoin")]
    pub fn magic(self) -> u32 {
        match self {
            Network::Litecoin => 0xDBB6_C0FB,
            Network::LitecoinTestnet => 0xFCC1_B7DC,
            Network::LitecoinRegtest => 0xDAB5_BFFA,
        }
    }

    #[cfg(feature = "dogecoin")]
    pub fn magic(self) -> u32 {
        match self {
            Network::Dogecoin => 0xC0C0_C0C0,
            Network::DogecoinTestnet => 0xFCC1_B7DC,
            Network::DogecoinRegtest => 0xDAB5_BFFA,
        }
    }

    pub fn is_regtest(self) -> bool {
        match self {
            #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
            Network::Regtest => true,
            #[cfg(feature = "liquid")]
            Network::LiquidRegtest => true,
            #[cfg(feature = "litecoin")]
            Network::LitecoinRegtest => true,
            #[cfg(feature = "dogecoin")]
            Network::DogecoinRegtest => true,
            _ => false,
        }
    }

    #[cfg(feature = "liquid")]
    pub fn address_params(self) -> &'static address::AddressParams {
        // Liquid regtest uses elements's address params
        match self {
            Network::Liquid => &address::AddressParams::LIQUID,
            Network::LiquidRegtest => &address::AddressParams::ELEMENTS,
            Network::LiquidTestnet => &address::AddressParams::LIQUID_TESTNET,
        }
    }

    #[cfg(feature = "liquid")]
    pub fn native_asset(self) -> &'static AssetId {
        match self {
            Network::Liquid => &*asset::NATIVE_ASSET_ID,
            Network::LiquidTestnet => &*asset::NATIVE_ASSET_ID_TESTNET,
            Network::LiquidRegtest => &*asset::NATIVE_ASSET_ID_REGTEST,
        }
    }

    #[cfg(feature = "liquid")]
    pub fn pegged_asset(self) -> Option<&'static AssetId> {
        match self {
            Network::Liquid => Some(&*asset::NATIVE_ASSET_ID),
            Network::LiquidTestnet | Network::LiquidRegtest => None,
        }
    }

    /// Returns the bech32 human-readable part for this network
    #[cfg(feature = "litecoin")]
    pub fn bech32_hrp(self) -> &'static str {
        match self {
            Network::Litecoin => "ltc",
            Network::LitecoinTestnet => "tltc",
            Network::LitecoinRegtest => "rltc",
        }
    }

    pub fn names() -> Vec<String> {
        #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
        return vec![
            "mainnet".to_string(),
            "testnet".to_string(),
            "testnet4".to_string(),
            "regtest".to_string(),
            "signet".to_string(),
        ];

        #[cfg(feature = "liquid")]
        return vec![
            "liquid".to_string(),
            "liquidtestnet".to_string(),
            "liquidregtest".to_string(),
        ];

        #[cfg(feature = "litecoin")]
        return vec![
            "litecoin".to_string(),
            "litecointestnet".to_string(),
            "litecoinregtest".to_string(),
        ];

        #[cfg(feature = "dogecoin")]
        return vec![
            "dogecoin".to_string(),
            "dogecointestnet".to_string(),
            "dogecoinregtest".to_string(),
        ];
    }
}

pub fn genesis_hash(network: Network) -> BlockHash {
    #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
    return bitcoin_genesis_hash(network.into());
    #[cfg(feature = "liquid")]
    return liquid_genesis_hash(network);
    #[cfg(feature = "litecoin")]
    return litecoin_genesis_hash(network);
    #[cfg(feature = "dogecoin")]
    return dogecoin_genesis_hash(network);
}

pub fn bitcoin_genesis_hash(network: BNetwork) -> bitcoin::BlockHash {
    lazy_static! {
        static ref BITCOIN_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Bitcoin).block_hash();
        static ref TESTNET_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Testnet).block_hash();
        static ref TESTNET4_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Testnet4).block_hash();
        static ref REGTEST_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Regtest).block_hash();
        static ref SIGNET_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Signet).block_hash();
    }
    match network {
        BNetwork::Bitcoin => *BITCOIN_GENESIS,
        BNetwork::Testnet => *TESTNET_GENESIS,
        BNetwork::Testnet4 => *TESTNET4_GENESIS,
        BNetwork::Regtest => *REGTEST_GENESIS,
        BNetwork::Signet => *SIGNET_GENESIS,
        _ => panic!("unknown network {:?}", network),
    }
}

#[cfg(feature = "liquid")]
pub fn liquid_genesis_hash(network: Network) -> elements::BlockHash {
    use crate::util::DEFAULT_BLOCKHASH;

    lazy_static! {
        static ref LIQUID_GENESIS: BlockHash =
            "1466275836220db2944ca059a3a10ef6fd2ea684b0688d2c379296888a206003"
                .parse()
                .unwrap();
    }

    match network {
        Network::Liquid => *LIQUID_GENESIS,
        // The genesis block for liquid regtest chains varies based on the chain configuration.
        // This instead uses an all zeroed-out hash, which doesn't matter in practice because its
        // only used for Electrum server discovery, which isn't active on regtest.
        _ => *DEFAULT_BLOCKHASH,
    }
}

#[cfg(feature = "litecoin")]
pub fn litecoin_genesis_hash(network: Network) -> BlockHash {
    lazy_static! {
        static ref LITECOIN_GENESIS: BlockHash =
            "12a765e31ffd4059bada1e25190f6e98c99d4028c0fe1666a68542019d52529c"
                .parse()
                .unwrap();
        static ref LITECOIN_TESTNET_GENESIS: BlockHash =
            "4966625a4b2851d9fdee139e56211a0d88575f59ed816ca060a99a68c5d4ee1d"
                .parse()
                .unwrap();
        // Litecoin regtest uses the same genesis block as Bitcoin regtest
        static ref LITECOIN_REGTEST_GENESIS: BlockHash =
            genesis_block(BNetwork::Regtest).block_hash();
    }

    match network {
        Network::Litecoin => *LITECOIN_GENESIS,
        Network::LitecoinTestnet => *LITECOIN_TESTNET_GENESIS,
        Network::LitecoinRegtest => *LITECOIN_REGTEST_GENESIS,
    }
}

#[cfg(feature = "dogecoin")]
pub fn dogecoin_genesis_hash(network: Network) -> BlockHash {
    lazy_static! {
        static ref DOGECOIN_GENESIS: BlockHash =
            "1a91e3dace36e2be3bf030a65679fe821aa1d6ef92e7c9902eb318182c355691"
                .parse()
                .unwrap();
        static ref DOGECOIN_TESTNET_GENESIS: BlockHash =
            "bb0a78264637406b6360aad926284d544d7049f45189db5664f3c4d07350559e"
                .parse()
                .unwrap();
        static ref DOGECOIN_REGTEST_GENESIS: BlockHash =
            "3d2160a3b5dc4a9d62e7e66a295f70313ac808440ef7400d6c0772171ce973a5"
                .parse()
                .unwrap();
    }

    match network {
        Network::Dogecoin => *DOGECOIN_GENESIS,
        Network::DogecoinTestnet => *DOGECOIN_TESTNET_GENESIS,
        Network::DogecoinRegtest => *DOGECOIN_REGTEST_GENESIS,
    }
}

/// Deserialize a block from raw bytes, handling network-specific block formats.
/// For Dogecoin, this skips AuxPoW data. For all other networks, this is a
/// straightforward call to the standard deserialize function.
#[cfg(not(any(feature = "liquid", feature = "dogecoin")))]
pub fn deserialize_block(data: &[u8]) -> Result<Block, bitcoin::consensus::encode::Error> {
    bitcoin::consensus::deserialize(data)
}

#[cfg(feature = "liquid")]
pub fn deserialize_block(data: &[u8]) -> Result<Block, elements::encode::Error> {
    elements::encode::deserialize(data)
}

#[cfg(feature = "dogecoin")]
pub fn deserialize_block(data: &[u8]) -> Result<Block, bitcoin::consensus::encode::Error> {
    crate::util::dogecoin::deserialize_dogecoin_block(data)
}

impl From<&str> for Network {
    fn from(network_name: &str) -> Self {
        match network_name {
            #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
            "mainnet" => Network::Bitcoin,
            #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
            "testnet" => Network::Testnet,
            #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
            "testnet4" => Network::Testnet4,
            #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
            "regtest" => Network::Regtest,
            #[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
            "signet" => Network::Signet,

            #[cfg(feature = "liquid")]
            "liquid" => Network::Liquid,
            #[cfg(feature = "liquid")]
            "liquidtestnet" => Network::LiquidTestnet,
            #[cfg(feature = "liquid")]
            "liquidregtest" => Network::LiquidRegtest,

            #[cfg(feature = "litecoin")]
            "litecoin" => Network::Litecoin,
            #[cfg(feature = "litecoin")]
            "litecointestnet" => Network::LitecoinTestnet,
            #[cfg(feature = "litecoin")]
            "litecoinregtest" => Network::LitecoinRegtest,

            #[cfg(feature = "dogecoin")]
            "dogecoin" => Network::Dogecoin,
            #[cfg(feature = "dogecoin")]
            "dogecointestnet" => Network::DogecoinTestnet,
            #[cfg(feature = "dogecoin")]
            "dogecoinregtest" => Network::DogecoinRegtest,

            _ => panic!("unsupported network: {:?}", network_name),
        }
    }
}

#[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
impl From<Network> for BNetwork {
    fn from(network: Network) -> Self {
        match network {
            Network::Bitcoin => BNetwork::Bitcoin,
            Network::Testnet => BNetwork::Testnet,
            Network::Testnet4 => BNetwork::Testnet4,
            Network::Regtest => BNetwork::Regtest,
            Network::Signet => BNetwork::Signet,
        }
    }
}

#[cfg(not(any(feature = "liquid", feature = "litecoin", feature = "dogecoin")))]
impl From<BNetwork> for Network {
    fn from(network: BNetwork) -> Self {
        match network {
            BNetwork::Bitcoin => Network::Bitcoin,
            BNetwork::Testnet => Network::Testnet,
            BNetwork::Testnet4 => Network::Testnet4,
            BNetwork::Regtest => Network::Regtest,
            BNetwork::Signet => Network::Signet,
            _ => panic!("unknown network {:?}", network),
        }
    }
}
