/**
* filename : integration_test
* author : HAMA
* date: 2025. 4. 30.
* description: 
**/

use futures::{executor::block_on, future, prelude::*, StreamExt};
use libp2p::{
  autonat, gossipsub, kad, noise,
  relay, swarm::{NetworkBehaviour, Swarm, SwarmEvent},
  core::transport::MemoryTransport,
  identity, multiaddr::Protocol,
  request_response::{cbor::Behaviour as CborBehaviour, ProtocolSupport},
  tcp, yamux, Multiaddr,
};
use std::time::Duration;

// ────────────────────────────────────────────────────────────────────────────
// 1. 공용 헬퍼: in-mem Swarm 만들기 (seed / chat 공통)
// ────────────────────────────────────────────────────────────────────────────
fn make_swarm<F, B>(behaviour_builder: F) -> Swarm<B>
where
  F: FnOnce(identity::Keypair) -> B,
  B: NetworkBehaviour + Unpin,
{
  // 고정 키
  let id_keys = identity::Keypair::generate_ed25519();
  
  // in-mem transport: MemoryTransport → Noise → Yamux
  let transport = MemoryTransport::default()
    .upgrade(libp2p::core::upgrade::Version::V1)
    .authenticate(noise::Config::new(&id_keys).unwrap())
    .multiplex(yamux::Config::default())
    .boxed();
  
  let behaviour = behaviour_builder(id_keys.clone());
  
  Swarm::with_executor(
    transport,
    behaviour,
    id_keys.public().to_peer_id(),
    |fut| { tokio::spawn(fut); }, // tokio executor
  )
}

// ────────────────────────────────────────────────────────────────────────────
// 2. Seed Behaviour: Kademlia + Relay(HOP)  (src/bin/seed.rs에 대응)
// ────────────────────────────────────────────────────────────────────────────
#[derive(NetworkBehaviour)]
struct SeedBehaviour {
  kademlia: kad::Behaviour<kad::store::MemoryStore>,
  relay:    relay::Behaviour,
}

fn build_seed_beh(kp: identity::Keypair) -> SeedBehaviour {
  let pid   = kp.public().to_peer_id();
  let store = kad::store::MemoryStore::new(pid);
  let mut kadb = kad::Behaviour::new(pid, store);
  kadb.set_mode(Some(kad::Mode::Server));
  
  let relayb = relay::Behaviour::new(pid, relay::Config::default());
  
  SeedBehaviour { kademlia: kadb, relay: relayb }
}

// ────────────────────────────────────────────────────────────────────────────
// 3. Chat Behaviour: Gossipsub + Kademlia + Relay-client  (src/bin/chat.rs)
// ────────────────────────────────────────────────────────────────────────────
#[derive(NetworkBehaviour)]
struct ChatBehaviour {
  gossipsub: gossipsub::Behaviour,
  kademlia:  kad::Behaviour<kad::store::MemoryStore>,
  relay:     relay::client::Behaviour,
}

fn build_chat_beh(kp: identity::Keypair) -> ChatBehaviour {
  let pid = kp.public().to_peer_id();
  
  // Gossipsub
  let gs_cfg = gossipsub::ConfigBuilder::default()
    .heartbeat_interval(Duration::from_secs(2))
    .build()
    .unwrap();
  let gossipsub = gossipsub::Behaviour::new(
    gossipsub::MessageAuthenticity::Signed(kp.clone()),
    gs_cfg,
  )
    .unwrap();
  
  // Kademlia
  let store     = kad::store::MemoryStore::new(pid);
  let kademlia  = kad::Behaviour::new(pid, store);
  
  // Relay client
  let relay_cfg = relay::client::Config::default();
  let relay     = relay::client::Behaviour::new(pid, relay_cfg);
  
  ChatBehaviour { gossipsub, kademlia, relay }
}

// ────────────────────────────────────────────────────────────────────────────
// 4. 테스트 1: Seed ↔ Chat 연결 · Kademlia 부트스트랩
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn seed_and_chat_bootstrap() {
  // ① 시드 노드 스웜 생성
  let mut seed_swarm = make_swarm(build_seed_beh);
  // MemoryTransport는 주소 `/memory/N` 으로 listen
  seed_swarm.listen_on("/memory/1".parse().unwrap()).unwrap();
  
  // ② 채팅 노드 스웜 생성
  let mut chat_swarm = make_swarm(build_chat_beh);
  chat_swarm.listen_on("/memory/2".parse().unwrap()).unwrap();
  
  // ③ Chat 노드가 Seed의 첫 리슨 주소를 Dial
  let seed_addr = loop {
    if let SwarmEvent::NewListenAddr { address, .. } = seed_swarm.select_next_some().await {
      break address;
    }
  };
  chat_swarm.dial(seed_addr.clone()).unwrap();
  
  // ④ Kademlia 부트스트랩 & 연결 확인 (최대 5초 wait)
  let mut success = false;
  let start = tokio::time::Instant::now();
  while start.elapsed() < Duration::from_secs(5) {
    tokio::select! {
            event = seed_swarm.select_next_some() => { /* seed side discard */ }
            event = chat_swarm.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                    if peer_id == seed_swarm.local_peer_id().clone() {
                        success = true;
                        break;
                    }
                }
            }
        }
  }
  assert!(success, "Chat 노드가 Seed에 연결되지 못했습니다");
}

// ────────────────────────────────────────────────────────────────────────────
// 5. 테스트 2: Gossipsub 메시지 교환
// ────────────────────────────────────────────────────────────────────────────
#[tokio::test]
async fn gossipsub_message_roundtrip() {
  // 시드
  let mut seed_swarm = make_swarm(build_seed_beh);
  seed_swarm.listen_on("/memory/3".parse().unwrap()).unwrap();
  
  // 채팅 A / B
  let mut alice = make_swarm(build_chat_beh);
  let mut bob   = make_swarm(build_chat_beh);
  alice.listen_on("/memory/4".parse().unwrap()).unwrap();
  bob.listen_on("/memory/5".parse().unwrap()).unwrap();
  
  // seed addr 확보 → dial
  let seed_addr = loop {
    if let SwarmEvent::NewListenAddr { address, .. } = seed_swarm.select_next_some().await {
      break address;
    }
  };
  alice.dial(seed_addr.clone()).unwrap();
  bob.dial(seed_addr).unwrap();
  
  // 토픽 구독
  let topic = gossipsub::IdentTopic::new("global-chat");
  alice.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
  bob  .behaviour_mut().gossipsub.subscribe(&topic).unwrap();
  
  // Alice 가 메시지 발행
  let payload = b"hello world!";
  tokio::spawn(async move {
    // 잠깐 기다린 뒤 publish
    tokio::time::sleep(Duration::from_millis(300)).await;
    alice.behaviour_mut().gossipsub.publish(topic.clone(), payload).unwrap();
    loop { alice.select_next_some().await; }
  });
  
  // Bob 이 받을 때까지 5초 타임아웃
  let mut received = false;
  let start = tokio::time::Instant::now();
  loop {
    if start.elapsed() > Duration::from_secs(5) {
      break;
    }
    tokio::select! {
            _ = seed_swarm.select_next_some() => {}
            _ = bob.select_next_some() => {
                if let Some(gossipsub::Event::Message { message, .. }) =
                    bob.behaviour_mut().gossipsub.next().now_or_never().flatten()
                {
                    if message.data == payload {
                        received = true;
                        break;
                    }
                }
            }
        }
  }
  assert!(received, "Bob 이 Gossipsub 메시지를 수신하지 못했습니다");
}
