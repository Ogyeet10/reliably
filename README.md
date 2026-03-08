# Reliably

A Rust client for [Ably](https://www.ably.com) with full REST and Realtime (Pub/Sub) support.

_[Ably](https://ably.com) is the platform that powers synchronized digital experiences in realtime. For more information, see the [Ably documentation](https://ably.com/documentation)._

This is a community-maintained fork of the original [ably-rust](https://github.com/ably/ably-rust) SDK, which only supported the REST API. This fork adds the complete Realtime (WebSocket) layer: persistent connections, channels with attach/detach, publish/subscribe, presence, and connection/channel state machines with automatic recovery.

## Features

### REST API
- Publish messages (string, JSON, binary)
- Retrieve message history with pagination
- Presence: get current members, history
- Token authentication (request tokens, sign token requests)
- Application statistics
- Message encryption (AES-128/AES-256)

### Realtime (Pub/Sub)
- Persistent WebSocket connection with automatic reconnection
- Connection state machine (initialized, connecting, connected, disconnected, suspended, closing, closed, failed)
- Channel state machine (initialized, attaching, attached, detaching, detached, suspended, failed)
- Publish with server ACK
- Subscribe with zero-message-loss delivery (unbounded per-subscriber channels)
- Multiple concurrent subscribers per channel, each with independent backpressure
- Presence: enter, leave, update, get members, subscribe to presence events
- Presence sync protocol with newness comparison and residual leave detection
- Automatic member re-enter on non-resumed re-attach (RTP17i)
- Connection resume with `connectionKey` and `connectionSerial`
- Connection state freshness check (`connectionStateTtl + maxIdleInterval`)
- Idle/heartbeat timeout detection (`maxIdleInterval` from server + grace period)
- Channel auto-re-attach on reconnection
- Discontinuity detection (RTL18) for zero-message-loss applications
- Ping/pong RTT measurement
- JSON and MessagePack wire protocols

### Design Decisions
- **Server-side, API-key auth only.** No token refresh, no `authCallback`/`authUrl` on WebSocket, no browser transports.
- **Zero message loss by default.** All subscriber delivery uses unbounded `mpsc` fan-out. Applications that need guaranteed delivery should also monitor `channel.on_discontinuity()` and backfill from the history API when fired.
- **No polling.** All state tracking uses `tokio::sync::watch` for race-free, immediate reads. State waits use `watch::Receiver::wait_for()`, not sleep loops.
- **Idiomatic async Rust.** Built on `tokio` with `tokio-tungstenite` for WebSocket transport.

## Installation

Add `reliably` and `tokio` to your `Cargo.toml`:

```toml
[dependencies]
reliably = "0.3.0"
tokio = { version = "1", features = ["full"] }
```

## Using the Realtime API

### Connect

```rust
use reliably::Realtime;

let client = Realtime::new("your-api-key")?;

// Wait for the connection to be established.
client.connection.wait_for_state(ConnectionState::Connected).await?;

// When done:
client.close().await;
```

Or with manual connect:

```rust
use reliably::{Realtime, ClientOptions, ConnectionState};

let mut opts = ClientOptions::new("your-api-key");
opts.auto_connect = false;

let client = Realtime::from_options(opts)?;
client.connection.connect();
client.connection.wait_for_state(ConnectionState::Connected).await?;
```

### Publish and Subscribe

```rust
use reliably::{Realtime, Data};

let client = Realtime::new("your-api-key")?;

let channel = client.channels.get("my-channel").await;
let mut sub = channel.subscribe().await?; // auto-attaches

// Publish (waits for server ACK).
channel.publish(Some("greeting"), Data::String("hello".into())).await?;

// Receive.
if let Some(msg) = sub.recv().await {
    println!("name={:?} data={:?}", msg.name, msg.data);
}
```

### Multiple Subscribers

Each subscriber gets its own independent unbounded stream. No message drops regardless of subscriber speed.

```rust
let mut sub1 = channel.subscribe().await?;
let mut sub2 = channel.subscribe().await?;

// Both sub1 and sub2 receive every message independently.
```

### JSON and Binary Data

```rust
use serde_json::json;

// JSON
channel.publish(Some("event"), Data::JSON(json!({"key": "value"}))).await?;

// Binary
let bytes = serde_bytes::ByteBuf::from(vec![0x01, 0x02, 0x03]);
channel.publish(Some("binary"), Data::Binary(bytes)).await?;
```

### Channel State

```rust
use reliably::ChannelState;

let channel = client.channels.get("my-channel").await;

// Synchronous (non-blocking) state check.
let state = channel.state(); // ChannelState::Initialized

// Explicit attach/detach.
channel.attach().await?;
assert_eq!(channel.state(), ChannelState::Attached);

channel.detach().await?;
assert_eq!(channel.state(), ChannelState::Detached);
```

### Connection State

```rust
use reliably::ConnectionState;

// Synchronous (non-blocking).
let state = client.connection.state();

// Wait with timeout.
client.connection.wait_for_state_with_timeout(
    ConnectionState::Connected,
    std::time::Duration::from_secs(10),
).await?;

// Ping.
let rtt = client.connection.ping().await?;
println!("RTT: {:?}", rtt);
```

### Presence

Presence requires a `client_id` set in `ClientOptions`:

```rust
use reliably::{Realtime, ClientOptions, Data};

let opts = ClientOptions::new("your-api-key")
    .client_id("my-user-id")?;
let client = Realtime::from_options(opts)?;

let channel = client.channels.get("chat-room").await;
channel.attach().await?;

// Enter presence.
channel.presence.enter(Some(Data::String("online".into()))).await?;

// Update presence data.
channel.presence.update(Some(Data::String("away".into()))).await?;

// Get all current members.
let members = channel.presence.get().await;
for member in &members {
    println!("{}: {:?}", member.client_id, member.data);
}

// Subscribe to presence events.
let mut presence_sub = channel.presence.subscribe();
if let Some(event) = presence_sub.recv().await {
    println!("{} did {:?}", event.client_id, event.action);
}

// Leave.
channel.presence.leave(None).await?;
```

### Discontinuity Detection (Zero Message Loss)

When a channel re-attaches without the server's `RESUMED` flag, messages may have been lost during the gap. Monitor `on_discontinuity()` and backfill from history:

```rust
let channel = client.channels.get("critical-channel").await;
let mut disc_rx = channel.on_discontinuity();

// Spawn a task to monitor for discontinuities.
tokio::spawn(async move {
    while let Some(event) = disc_rx.recv().await {
        eprintln!("Discontinuity detected! resumed={}, reason={:?}", event.resumed, event.reason);
        // Backfill from history API here.
    }
});
```

## Using the REST API

### Initialize a Client

```rust
// With an API key:
let client = reliably::Rest::new("xVLyHw.SmDuMg:************")?;

// With an auth URL:
let auth_url = "https://example.com/auth".parse()?;
let client = reliably::ClientOptions::new()
    .auth_url(auth_url)
    .rest()?;
```

### Publish a Message

```rust
let channel = client.channels().get("test");

// String
channel.publish().string("a string").send().await?;

// JSON
#[derive(Serialize)]
struct Point { x: i32, y: i32 }
channel.publish().json(Point { x: 3, y: 4 }).send().await?;

// Binary
channel.publish().binary(vec![0x01, 0x02, 0x03]).send().await?;
```

### Retrieve History

```rust
let mut pages = channel.history().pages();
while let Some(Ok(page)) = pages.next().await {
    for msg in page.items().await? {
        println!("message data = {:?}", msg.data);
    }
}
```

### Retrieve Presence

```rust
let mut pages = channel.presence.get().pages();
while let Some(Ok(page)) = pages.next().await {
    for msg in page.items().await? {
        println!("presence data = {:?}", msg.data);
    }
}
```

### Encrypted Messages

```rust
let cipher_key = reliably::crypto::generate_random_key::<reliably::crypto::Key256>();
let params = reliably::rest::CipherParams::from(cipher_key);
let channel = client.channels().name("encrypted").cipher(params).get();

channel.publish().string("sensitive data is encrypted").send().await?;
```

### Request a Token

```rust
let token = client
    .auth()
    .request_token()
    .client_id("test@example.com")
    .capability(r#"{"example":["subscribe"]}"#)
    .send()
    .await?;
```

### Application Statistics

```rust
let mut pages = client.stats().pages();
while let Some(Ok(page)) = pages.next().await {
    for stats in page.items().await? {
        println!("stats = {:?}", stats);
    }
}
```

## Architecture

```
src/
  lib.rs                 # Public API, re-exports, test suite
  options.rs             # ClientOptions (shared by REST and Realtime)
  rest.rs                # REST client, channels, messages, encoding/decoding
  auth.rs                # Token auth, API key signing
  realtime.rs            # Realtime client entry point, router task
  connection.rs          # Connection state machine, ConnectionManager event loop
  realtime_channel.rs    # Channel state machine, pub/sub, discontinuity detection
  realtime_presence.rs   # PresenceMap, sync protocol, enter/leave/update API
  protocol.rs            # Wire format: actions, flags, ProtocolMessage, MessageQueue
  transport.rs           # WebSocket transport (tokio-tungstenite)
  crypto.rs              # AES-CBC encryption/decryption
  http.rs                # HTTP request builder, pagination
  presence.rs            # REST presence API
  stats.rs               # Application statistics types
  error.rs               # Error types and codes
```

## What's Not Implemented

- **Token auth refresh on WebSocket** -- No `authCallback`/`authUrl` re-auth via AUTH protocol messages. API key auth only.
- **Recovery keys** -- No cross-process resume. Process restart triggers discontinuity; backfill from history.
- **Comet/long-polling** -- WebSocket only.
- **Delta compression** -- No vcdiff delta decoding.
- **LiveObjects, Annotations, message interactions** -- Not in scope.
- **Filtered subscriptions** -- Not implemented.
- **Browser-specific concerns** -- Server-side only.

## Testing

Tests run against the Ably sandbox environment:

```sh
cargo test
```

The test suite includes 59 integration tests (40 REST + 19 Realtime) and 13 doctests:

- REST: publish, history, presence, auth, tokens, stats, encryption, fallback hosts
- Realtime: connect, close, ping, channel attach/detach, publish/subscribe (string, JSON, binary), multiple subscribers, high-throughput ordered delivery, two-client cross-connection pub/sub, auto-attach on subscribe, channel state changes, presence enter/leave/update/get with multiple clients, discontinuity detection

## License

Apache-2.0
