# xComm – 탈중앙 P2P 

**xComm** 은 **Rust + libp2p** 기반의 완전 P2P 애플리케이션입니다.  
최소 2 대의 **Seed 노드**(Kademlia DHT 서버 + Relay v2 HOP)만 24H 가동하면  
모든 사용자는 **“앱을 켜자마자 곧바로 전원 채팅방”** 에 입장할 수 있습니다.

> **지원 규모**: 동시 ≈ 100 명 | **필요 서버**: Seed 2–3 대(512 MiB VM면 충분)

## [UPnP 시도] → 성공 → 직접 연결
↘ 실패 → [AutoNAT 감지] → [릴레이 연결] → [DCUtR 시도] → 성공 → 직접 연결
↘ 실패 → 릴레이 유지
---

## 🌟 주요 특징

| 기능 | 설명 |
|------|------|
| **Gossipsub v1.2** | 메시지를 네트워크 전체에 브로드캐스트 (중복 제거·스팸 스코어링) |
| **Kademlia DHT** | Seed 노드 한 곳만 알아도 전 피어 탐색·부트스트랩 |
| **Relay v2 (HOP)** | 다중 NAT 환경 사용자를 위해 자동 릴레이 경로 제공 |
| **AutoNAT + UPnP** | 퍼블릭 포트 가능 시 직접 연결, 불가 시 Relay 경유 결정 |
| **MemoryTransport 테스트** | 실제 포트 없이 통합 테스트 가능 (CI 친화적) |

---

## 📂 프로젝트 구조

```text
xcomm/
├── Cargo.toml          # 모든 feature 수동 지정
├── src/
│   └── bin/
│       ├── chat.rs     # 일반 노드(클라이언트)
│       └── seed.rs     # Seed + Relay 서버
└── tests/
    └── integration.rs  # in-memory 통합 테스트 2종
```

## 🚀 빠른 시작

### 1. 필수 조건
| 구분          | 요구 사항                                       |
| -------------- | ---------------------------------------------- |
| **Rust**       | 1.74 이상                                       |
| **Seed 노드**  | 고정 IP 또는 DNS 2 대 이상, TCP/UDP 포트 **4001** 개방 |
| **클라이언트** | 인터넷 연결만 있으면 됨 (NAT 환경 가능)          |

### 2. 클론 & 빌드
```bash
git clone https://github.com/your-org/xcomm.git
cd xcomm
cargo build --release
```

### 3. Seed 노드 실행 (고정 서버 ≥ 2 대)

```bash
# 첫 실행: PeerId가 출력되고 새 키가 생성됩니다.
RUST_LOG=info cargo run --bin seed --release
# ▶ Listen on /ip4/203.0.113.10/tcp/4001/p2p/12D3KooWSeed1
```
같은 Peer ID로 항상 부팅하려면, hex-encoded protobuf 프라이빗 키를
SEED_PRIVKEY 환경변수에 저장해 실행하세요.

```bash
export SEED_PRIVKEY=<hex-protobuf-private-key>
RUST_LOG=info cargo run --bin seed --release
```
(Seed 주소를 DNS로 배포하고 싶다면 dnsaddr TXT 레코드에 위 Multiaddr를 등록합니다.)

### 4. 클라이언트 노드 실행

```bash
# 방법 ①: Seed 주소를 CLI 인자로 직접 입력
RUST_LOG=info cargo run --bin chat --release \
  /ip4/203.0.113.10/tcp/4001/p2p/12D3KooWSeed1

# 방법 ②: Seed 주소가 코드에 하드코딩되어 있다면
RUST_LOG=info cargo run --bin chat --release
```
터미널에 입력하는 모든 줄이 실시간으로 전체에 전파됩니다.

### 5. 테스트

```bash
cargo test --all

```
seed_and_chat_bootstrap	
- Chat 노드가 Seed 노드에 다이얼하고 Kademlia 부트스트랩이 성공하는지 확인

gossipsub_message_roundtrip	
- Alice가 publish한 메시지를 Bob이 subscribe로 정상 수신하는지 검증

테스트는 MemoryTransport 로 실행되므로 네트워크 포트가 필요 없습니다..


### 6. 실행예시

```bash
🚀 Seed PeerId: 12D3KooWSeed1
▶ Listen on /ip4/203.0.113.10/tcp/4001/p2p/12D3KooWSeed1

📡 Local PeerId: 12D3KooWChatA
▶ Listen: /ip4/192.168.0.11/tcp/49174
🌐 External addr: /ip4/14.36.108.162/tcp/49174
[12D3KooWChatB] 안녕하세요!

```


### 7. 주의사항

- Seed 노드 꺼지면 새 참가자가 부트스트랩할 수 없습니다.
최소 2 대를 24 H 유지하세요.

- 라우터 UPnP가 꺼져 있거나 이중 NAT 환경이면 Relay 경유로만 연결됩니다.
(자동으로 판단되므로 사용자가 신경 쓸 필요는 없습니다.)

- Gossipsub 메시지 크기를 2 KB 이하로 제한해 두었습니다.
필요 시 ConfigBuilder::max_transmit_size() 를 조정 바랍니다.
