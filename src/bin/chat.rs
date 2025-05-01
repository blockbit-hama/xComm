/**
* filename : chat
* author   : HAMA
* date     : 2025-04-28
* description : 일반 노드 (UPnP → AutoNAT → Relay → DCUtR 업그레이드)
*/

use futures::{prelude::*, StreamExt};
use libp2p::{
  autonat,
  dcutr,
  gossipsub, kad, mdns,
  noise, relay, tcp, upnp, yamux,
  swarm::{NetworkBehaviour, SwarmEvent},
  Multiaddr, SwarmBuilder,
};
use std::{
  collections::hash_map::DefaultHasher,
  error::Error,
  hash::{Hash, Hasher},
  time::Duration,
};
use tokio::io::{self, AsyncBufReadExt};
use tracing_subscriber::EnvFilter;

/* ───── Behaviour 정의 ───── */
#[derive(NetworkBehaviour)]
#[behaviour(
  event_process = false   // 수동 match를 위해 derive 이벤트 합치기
)]
struct ChatBehaviour {
  gossipsub: gossipsub::Behaviour,
  kademlia:  kad::Behaviour<kad::store::MemoryStore>,
  mdns:      mdns::tokio::Behaviour,
  upnp:      upnp::tokio::Behaviour,
  autonat:   autonat::Behaviour,
  relay:     relay::client::Behaviour,
  dcutr:     dcutr::Behaviour,
}

/* ───── main ───── */
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();
  
  /* 키 생성 */
  let id_keys = libp2p::identity::Keypair::generate_ed25519();
  let peer_id = id_keys.public().to_peer_id();
  println!("📡 Local PeerId: {peer_id}");
  
  /* SwarmBuilder */
  let mut swarm = SwarmBuilder::with_existing_identity(id_keys)
    .with_tokio()
    .with_tcp(
      tcp::Config::default(),
      noise::Config::new,
      yamux::Config::default,
    )?
    .with_quic()
    .with_relay_client(noise::Config::new, yamux::Config::default)?
    .with_behaviour(|keypair| {
      /* 1️⃣ Gossipsub */
      let msg_id_fn = |m: &gossipsub::Message| {
        let mut h = DefaultHasher::new();
        m.data.hash(&mut h);
        gossipsub::MessageId::from(h.finish().to_string())
      };
      let gs_cfg = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(4))
        .mesh_n_high(12)
        .validation_mode(gossipsub::ValidationMode::Strict)
        .message_id_fn(msg_id_fn)
        .build()?;
      let gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(keypair.clone()),
        gs_cfg,
      )?;
      
      /* 2️⃣ Kademlia */
      let pid = keypair.public().to_peer_id();
      let store = kad::store::MemoryStore::new(pid);
      let kademlia = kad::Behaviour::new(pid, store);
      
      /* 3️⃣ 기타 */
      let mdns  = mdns::tokio::Behaviour::new(mdns::Config::default(), pid)?;
      let upnp  = upnp::tokio::Behaviour::default();
      let autonat = autonat::Behaviour::new(pid, Default::default());
      let relay = relay::client::Behaviour::new(pid, relay::client::Config::default());
      let dcutr = dcutr::Behaviour::new(pid);
      
      Ok(ChatBehaviour {
        gossipsub,
        kademlia,
        mdns,
        upnp,
        autonat,
        relay,
        dcutr,
      })
    })?
    .build();
  
  /* 리스닝 */
  swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
  swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
  
  /* 토픽 */
  let topic = gossipsub::IdentTopic::new("global-chat");
  swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
  
  /* CLI 멀티애드 dial */
  for arg in std::env::args().skip(1) {
    if let Ok(addr) = arg.parse::<Multiaddr>() {
      println!("➡ Dial CLI addr: {addr}");
      swarm.dial(addr)?;
    }
  }
  /* 하드코딩 Seed 다이얼 */
  const SEED_LIST: &[&str] = &[
    "/dnsaddr/seed1.xcomm.org/tcp/4001/p2p/12D3KooWSeed1",
    "/dnsaddr/seed2.xcomm.org/tcp/4001/p2p/12D3KooWSeed2",
  ];
  for s in SEED_LIST {
    if let Ok(addr) = s.parse() {
      swarm.dial(addr)?;
    }
  }
  
  println!("🗣️  채팅 애플리케이션 시작. 메시지를 입력하세요…");
  
  let mut stdin = io::BufReader::new(io::stdin()).lines();
  
  loop {
    tokio::select! {
            Ok(Some(line)) = stdin.next_line() => {
                swarm.behaviour_mut().gossipsub.publish(topic.clone(), line.as_bytes())?;
            }
            event = swarm.select_next_some() => match event {
                /* ────────── Gossipsub ────────── */
                SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(
                    gossipsub::Event::Message { propagation_source, message, .. }
                )) => println!("[{propagation_source}] {}", String::from_utf8_lossy(&message.data)),

                /* ────────── UPnP & AutoNAT ────────── */
                SwarmEvent::Behaviour(ChatBehaviourEvent::Upnp(upnp::Event::NewExternalAddr(a))) =>
                    println!("🌐 UPnP external addr: {a}"),
                SwarmEvent::Behaviour(ChatBehaviourEvent::Autonat(
                    autonat::Event::StatusChanged { old, new }
                )) => println!("🔍 AutoNAT: {old:?} → {new:?}"),

                /* ────────── DCUtR ────────── */
                SwarmEvent::Behaviour(ChatBehaviourEvent::Dcutr(
                    dcutr::Event::InboundUpgradeSucceeded { peer_id, .. }
                )) |
                SwarmEvent::Behaviour(ChatBehaviourEvent::Dcutr(
                    dcutr::Event::OutboundUpgradeSucceeded { peer_id, .. }
                )) => println!("🚀 DCUtR direct connection with {peer_id} established"),

                SwarmEvent::NewListenAddr { address, .. } =>
                    println!("▶ Listen: {address}"),

                _ => {} // 기타 이벤트는 생략
            }
        }
  }
}
