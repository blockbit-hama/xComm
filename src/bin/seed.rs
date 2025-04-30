/**
* filename : seed
* author : HAMA
* date: 2025. 4. 30.
* description: 
**/

//! xComm Seed / Relay 노드
//! 고정 IP 또는 도메인에서 24시간 실행하세요.

use futures::StreamExt;
use libp2p::{
  kad, noise, relay, tcp, yamux, quic,
  swarm::{NetworkBehaviour, SwarmEvent},
  PeerId, SwarmBuilder,
};
use std::{env, error::Error, time::Duration};
use tracing_subscriber::EnvFilter;

/* ───── Behaviour 정의 ───── */
#[derive(NetworkBehaviour)]
struct SeedBehaviour {
  kademlia: kad::Behaviour<kad::store::MemoryStore>,
  relay:    relay::Behaviour, // 서버(HOP) 모드
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();
  
  /* ── 1) 고정 키 ── */
  let id_keys = load_or_gen_keypair();
  println!("🚀 Seed PeerId: {peer_id}");
  
  /* ── 2) Swarm 빌드 ── */
  let mut swarm = SwarmBuilder::with_existing_identity(id_keys)
    .with_tokio()
    .with_tcp(
      tcp::Config::default(),
      noise::Config::new,
      yamux::Config::default,
    )?
    .with_quic()
    .with_behaviour(|keypair| {
      /* Kademlia 서버 모드 */
      let peer_id = keypair.public().to_peer_id();
      
      let store = kad::store::MemoryStore::new(peer_id);
      let mut kad_beh = kad::Behaviour::new(peer_id, store);
      kad_beh.set_mode(Some(kad::Mode::Server));
      
      /* Relay v2 HOP */
      let relay_beh = relay::Behaviour::new(peer_id, relay::Config::default());
      
      Ok(SeedBehaviour { kademlia: kad_beh, relay: relay_beh })
    })?
    .build();
  
  /* ── 3) 고정 포트 리스닝 ── */
  swarm.listen_on("/ip4/0.0.0.0/tcp/4001".parse()?)?;
  swarm.listen_on("/ip4/0.0.0.0/udp/4001/quic-v1".parse()?)?;
  
  /* ── 4) 루프 ── */
  loop {
    if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
      println!("▶ Listen on {address}");
    }
  }
}

/* ───── 키 로딩/생성 헬퍼 ───── */
fn load_or_gen_keypair() -> libp2p::identity::Keypair {
  if let Ok(hex) = env::var("SEED_PRIVKEY") {
    let bytes = hex::decode(hex).expect("hex decode");
    libp2p::identity::Keypair::from_protobuf_encoding(&bytes)
      .expect("valid protobuf privkey")
  } else {
    let kp = libp2p::identity::Keypair::generate_ed25519();
    eprintln!("⚠ 새 키 생성됨 – 동일 ID 유지하려면 SEED_PRIVKEY 환경변수에 저장하세요.");
    kp
  }
}
