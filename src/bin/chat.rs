/**
* filename : chat
* author : HAMA
* date: 2025. 4. 28.
* description: 
**/

use futures::{prelude::*, StreamExt};
use libp2p::{
  autonat,
  gossipsub, kad, mdns,
  noise, relay, tcp, upnp, yamux,
  swarm::{NetworkBehaviour, SwarmEvent},
  Multiaddr, PeerId, SwarmBuilder,
};
use std::{collections::hash_map::DefaultHasher, error::Error, hash::{Hash, Hasher}, time::Duration};
use tokio::io::{self, AsyncBufReadExt};
use tracing_subscriber::EnvFilter;

/* ───── 네트워크 Behaviour 정의 ───── */
#[derive(NetworkBehaviour)]
struct ChatBehaviour {
  gossipsub: gossipsub::Behaviour,
  kademlia: kad::Behaviour<kad::store::MemoryStore>,
  mdns:      mdns::tokio::Behaviour,
  upnp:      upnp::tokio::Behaviour,
  autonat:   autonat::Behaviour,
}

/* ───── 실행 진입점 ───── */
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();
  
  /* 키 쌍 생성(임시) */
  let id_keys = libp2p::identity::Keypair::generate_ed25519();
  let peer_id = id_keys.public().to_peer_id();
  println!("📡 Local PeerId: {peer_id}");
  
  /* Swarm 빌드 */
  let mut swarm = SwarmBuilder::with_existing_identity(id_keys)
    .with_tokio()
    .with_tcp(
      tcp::Config::default(),
      noise::Config::new,
      yamux::Config::default,
    )?
    .with_quic()
    .with_relay_client(
      noise::Config::new,                      // security upgrade
      yamux::Config::default,                  // multiplexer upgrade
    )?
    .with_behaviour(|key| {
      /* ── 1) Gossipsub ── */
      let message_id_fn = |m: &gossipsub::Message| {
        let mut h = DefaultHasher::new();
        m.data.hash(&mut h);
        gossipsub::MessageId::from(h.finish().to_string())
      };
      let gs_cfg = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(4))
        .mesh_n_high(12)
        .validation_mode(gossipsub::ValidationMode::Strict)
        .message_id_fn(message_id_fn)
        .build()?;
      let gossipsub =
        gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Signed(key.clone()), gs_cfg)?;
      
      /* ── 2) Kademlia ── */
      let store = kad::store::MemoryStore::new(key.public().to_peer_id());
      let kademlia = kad::Behaviour::new(key.public().to_peer_id(), store);
      
      /* ── 3) 기타 ── */
      let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
      let upnp = upnp::tokio::Behaviour::default();
      let autonat = autonat::Behaviour::new(key.public().to_peer_id(), Default::default());
      
      Ok(ChatBehaviour { gossipsub, kademlia, mdns, upnp, autonat })
    })?
    .build();
  
  /* 리스닝 시작 */
  swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
  swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
  
  /* 토픽 구독 */
  let topic = gossipsub::IdentTopic::new("global-chat");
  swarm.behaviour_mut().gossipsub.subscribe(&topic)?;
  
  /* ── 1) CLI 인자에 멀티애드가 있으면 Dial ── */
  for arg in std::env::args().skip(1) {
    if let Ok(addr) = arg.parse::<Multiaddr>() {
      println!("➡ Dial CLI addr: {addr}");
      swarm.dial(addr)?;
    }
  }
  
  /* ── 2) 하드코딩 Seed 리스트 Dial ── */
  const SEED_LIST: &[&str] = &[
    "/dnsaddr/seed1.xcomm.org/tcp/4001/p2p/12D3KooWSeed1",
    "/dnsaddr/seed2.xcomm.org/tcp/4001/p2p/12D3KooWSeed2",
  ];
  for s in SEED_LIST {
    if let Ok(addr) = s.parse() {
      swarm.dial(addr)?;
    }
  }
  
  println!("🗣️  채팅 애플리케이션 시작됨. 메시지를 입력하세요...");
  
  let mut stdin = io::BufReader::new(io::stdin()).lines();
  
  loop {
    tokio::select! {
            Ok(Some(line)) = stdin.next_line() => {
                swarm.behaviour_mut().gossipsub.publish(topic.clone(), line.as_bytes())?;
            }
            event = swarm.select_next_some() => match event {
                /* ───── 주요 이벤트 처리 ───── */
                SwarmEvent::Behaviour(ChatBehaviourEvent::Gossipsub(
                    gossipsub::Event::Message { propagation_source, message, .. }
                )) => {
                    println!("[{propagation_source}] {}", String::from_utf8_lossy(&message.data));
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("▶ Listen: {address}");
                }
                SwarmEvent::Behaviour(ChatBehaviourEvent::Upnp(upnp::Event::NewExternalAddr(a))) =>
                    println!("🌐 External addr: {a}"),
                _ => {} // 세부 로그는 생략
            }
        }
  }
}