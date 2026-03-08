# Reliably

A fully-featured real-time Rust client for [Ably](https://www.ably.com).

_[Ably](https://ably.com) is the platform that powers synchronized digital experiences in realtime. For more information, see the [Ably documentation](https://ably.com/documentation)._

This is a community-maintained fork of the original [ably-rust](https://github.com/ably/ably-rust) SDK.

## Installation

Add the `reliably` and `tokio` crates to your `Cargo.toml`:

```
[dependencies]
reliably = "0.2.0"
tokio = { version = "1", features = ["full"] }
```
[dependencies]
ably = "0.2.0"
tokio = { version = "1", features = ["full"] }
```

## Using the REST API

### Initialize A Client

Initialize a client with a method to authenticate with Ably.

- With an API Key:

```rust
let client = reliably::Rest::from("xVLyHw.SmDuMg:************");
```

- With an auth URL:

```rust
let auth_url = "https://example.com/auth".parse()?;

let client = reliably::ClientOptions::new().auth_url(auth_url).client()?;
```

### Publish A Message

Given:

```rust
let channel = client.channels.get("test");
```

- Publish a string:

```rust
let result = channel.publish().string("a string").send().await;
```

- Publish a JSON object:

```rust
#[derive(Serialize)]
struct Point {
    x: i32,
    y: i32,
}
let point = Point { x: 3, y: 4 };
let result = channel.publish().json(point).send().await;
```

- Publish binary data:

```rust
let data = vec![0x01, 0x02, 0x03, 0x04];
let result = channel.publish().binary(data).send().await;
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

### Retrieve Presence History

```rust
let mut pages = channel.presence.history().pages();
while let Some(Ok(page)) = pages.next().await {
    for msg in page.items().await? {
        println!("presence data = {:?}", msg.data);
    }
}
```

### Encrypted Message Data

When a 128 bit or 256 bit key is provided to the library, the data attributes of all messages are encrypted and decrypted automatically using that key. The secret key is never transmitted to Ably. See https://www.ably.com/documentation/realtime/encryption

```rust
// Initialize a channel with cipher parameters so that published messages
// get encrypted.
let cipher_key = reliably::crypto::generate_random_key::<reliably::crypto::Key256>();
let params = reliably::rest::CipherParams::from(cipher_key);
let channel = client.channels.name("rust-example").cipher(params).get();

channel
    .publish()
    .name("name is not encrypted")
    .string("sensitive data is encrypted")
    .send()
    .await;
```


### Request A Token

```rust
let result = client
    .auth
    .request_token()
    .client_id("test@example.com")
    .capability(r#"{"example":["subscribe"]}"#)
    .send()
    .await;
```

### Retrieve Application Statistics

```rust
let mut pages = client.stats().pages();
while let Some(Ok(page)) = pages.next().await {
    for stats in page.items().await? {
        println!("stats = {:?}", stats);
    }
}
```
