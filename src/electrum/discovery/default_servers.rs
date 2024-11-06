use crate::chain::Network;
use crate::electrum::discovery::{DiscoveryManager, Service};

macro_rules! add_servers {
    ($discovery:expr, $servers:expr) => {
        for (address, services) in $servers.iter() {
            $discovery
                .add_default_server(address.clone().into(), services.clone())
                .ok();
        }
    };
}

pub fn add_default_servers(discovery: &DiscoveryManager, network: Network) {
    match network {
        #[cfg(not(feature = "liquid"))]
        Network::Bitcoin => {
            let servers = vec![
                (
                    "3smoooajg7qqac2y.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "81-7-10-251.blue.kundencontroller.de",
                    vec![Service::Ssl(50002)],
                ),
                (
                    "E-X.not.fyi",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "VPS.hsmiths.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("b.ooze.cc", vec![Service::Tcp(50001), Service::Ssl(50002)]),
                (
                    "bauerjda5hnedjam.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "bauerjhejlv6di7s.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "bitcoin.corgi.party",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "bitcoin3nqy3db7c.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "bitcoins.sk",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "btc.cihar.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "btc.xskyx.net",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "currentlane.lovebitco.in",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "daedalus.bauerj.eu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum.jochen-hoenicke.de",
                    vec![Service::Tcp(50003), Service::Ssl(50005)],
                ),
                ("dragon085.startdedicated.de", vec![Service::Ssl(50002)]),
                (
                    "e-1.claudioboxx.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("e.keff.org", vec![Service::Tcp(50001), Service::Ssl(50002)]),
                (
                    "electrum-server.ninja",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum-unlimited.criptolayer.net",
                    vec![Service::Ssl(50002)],
                ),
                (
                    "electrum.eff.ro",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum.festivaldelhumor.org",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum.hsmiths.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum.leblancnet.us",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("electrum.mindspot.org", vec![Service::Ssl(50002)]),
                (
                    "electrum.qtornado.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("electrum.taborsky.cz", vec![Service::Ssl(50002)]),
                (
                    "electrum.villocq.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum2.eff.ro",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum2.villocq.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrumx.bot.nu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrumx.ddns.net",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("electrumx.ftp.sh", vec![Service::Ssl(50002)]),
                (
                    "electrumx.ml",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrumx.soon.it",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("electrumxhqdsmlu.onion", vec![Service::Tcp(50001)]),
                (
                    "elx01.knas.systems",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "enode.duckdns.org",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "fedaykin.goip.de",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("fn.48.org", vec![Service::Tcp(50003), Service::Ssl(50002)]),
                (
                    "helicarrier.bauerj.eu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "hsmiths4fyqlw5xw.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "hsmiths5mjk6uijs.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "icarus.tetradrachm.net",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "electrum.emzy.de",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "ndnd.selfhost.eu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("ndndword5lpb7eex.onion", vec![Service::Tcp(50001)]),
                (
                    "orannis.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "ozahtqwp25chjdjd.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "qtornadoklbgdyww.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("rbx.curalle.ovh", vec![Service::Ssl(50002)]),
                ("s7clinmo4cazmhul.onion", vec![Service::Tcp(50001)]),
                (
                    "tardis.bauerj.eu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("technetium.network", vec![Service::Ssl(50002)]),
                (
                    "tomscryptos.com",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "ulrichard.ch",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "vmd27610.contaboserver.net",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "vmd30612.contaboserver.net",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                (
                    "wsw6tua3xl24gsmi264zaep6seppjyrkyucpsmuxnjzyt3f3j6swshad.onion",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("xray587.startdedicated.de", vec![Service::Ssl(50002)]),
                ("yuio.top", vec![Service::Tcp(50001), Service::Ssl(50002)]),
                (
                    "bitcoin.dragon.zone",
                    vec![Service::Tcp(50003), Service::Ssl(50004)],
                ),
                ("ecdsa.net", vec![Service::Tcp(50001), Service::Ssl(110)]),
                ("btc.usebsv.com", vec![Service::Ssl(50006)]),
                (
                    "e2.keff.org",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("electrum.hodlister.co", vec![Service::Ssl(50002)]),
                ("electrum3.hodlister.co", vec![Service::Ssl(50002)]),
                ("electrum5.hodlister.co", vec![Service::Ssl(50002)]),
                ("electrumx.electricnewyear.net", vec![Service::Ssl(50002)]),
                (
                    "fortress.qtornado.com",
                    vec![Service::Tcp(50001), Service::Ssl(443)],
                ),
                (
                    "green-gold.westeurope.cloudapp.azure.com",
                    vec![Service::Tcp(56001), Service::Ssl(56002)],
                ),
                (
                    "electrumx.erbium.eu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
            ];
            add_servers!(discovery, servers);
        }
        #[cfg(not(feature = "liquid"))]
        Network::Testnet => {
            let servers = vec![
                (
                    "hsmithsxurybd7uh.onion",
                    vec![Service::Tcp(53011), Service::Ssl(53012)],
                ),
                (
                    "testnet.hsmiths.com",
                    vec![Service::Tcp(53011), Service::Ssl(53012)],
                ),
                (
                    "testnet.qtornado.com",
                    vec![Service::Tcp(51001), Service::Ssl(51002)],
                ),
                (
                    "testnet1.bauerj.eu",
                    vec![Service::Tcp(50001), Service::Ssl(50002)],
                ),
                ("tn.not.fyi", vec![Service::Tcp(55001), Service::Ssl(55002)]),
                (
                    "bitcoin.cluelessperson.com",
                    vec![Service::Tcp(51001), Service::Ssl(51002)],
                ),
            ];
            add_servers!(discovery, servers);
        }
        _ => (),
    }
}
