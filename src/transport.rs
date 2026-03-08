//! WebSocket transport for the Ably realtime protocol.
//!
//! Manages a single WebSocket connection: connecting, serialising/deserialising
//! protocol messages, and detecting idle connections via heartbeat timers.

use std::time::Duration;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::{connect_async, MaybeTlsStream};
use url::Url;

use crate::auth;
use crate::error::{Error, ErrorCode};
use crate::options::ClientOptions;
use crate::protocol::{ProtocolMessage, PROTOCOL_VERSION};
use crate::rest::Format;
use crate::Result;

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ---------------------------------------------------------------------------
// Transport events sent to the ConnectionManager
// ---------------------------------------------------------------------------

/// Events produced by the transport and consumed by the connection manager.
#[derive(Debug)]
pub(crate) enum TransportEvent {
    /// A protocol message was received from the server.
    Message(ProtocolMessage),
    /// The transport was disconnected (cleanly or due to error).
    Disconnected(Option<Error>),
}

// ---------------------------------------------------------------------------
// Transport handle
// ---------------------------------------------------------------------------

/// Handle to a live WebSocket transport.
///
/// The transport runs as two background tasks (read + write). The handle
/// provides methods to send protocol messages and to shut down the transport.
pub(crate) struct Transport {
    /// Send protocol messages to the write task.
    write_tx: mpsc::Sender<ProtocolMessage>,
    /// Read task handle.
    read_handle: JoinHandle<()>,
    /// Write task handle.
    write_handle: JoinHandle<()>,
}

impl Transport {
    /// Open a WebSocket connection to the Ably realtime endpoint.
    ///
    /// Returns a `Transport` handle and immediately starts pumping messages
    /// to `event_tx`.
    pub async fn connect(
        opts: &ClientOptions,
        resume_key: Option<&str>,
        event_tx: mpsc::Sender<TransportEvent>,
    ) -> Result<Self> {
        let url = build_ws_url(opts, resume_key)?;
        let format = opts.format;

        let (ws_stream, _response) = connect_async(url.as_str())
            .await
            .map_err(|e| Error::with_cause(ErrorCode::ConnectionFailed, e, "WebSocket connect failed"))?;

        let (sink, stream) = ws_stream.split();

        // Channel for sending outbound protocol messages.
        let (write_tx, write_rx) = mpsc::channel::<ProtocolMessage>(64);

        // Spawn read task.
        let read_handle = {
            let event_tx = event_tx.clone();
            let fmt = format;
            tokio::spawn(async move {
                read_loop(stream, event_tx, fmt).await;
            })
        };

        // Spawn write task.
        let write_handle = {
            let fmt = format;
            tokio::spawn(async move {
                write_loop(sink, write_rx, fmt).await;
            })
        };

        Ok(Self {
            write_tx,
            read_handle,
            write_handle,
        })
    }

    /// Send a protocol message over the WebSocket.
    pub async fn send(&self, msg: ProtocolMessage) -> Result<()> {
        self.write_tx
            .send(msg)
            .await
            .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "Transport write channel closed"))
    }

    /// Initiate a graceful close by dropping the write channel and aborting
    /// the read task.
    pub fn close(self) {
        // Dropping write_tx causes the write loop to exit, which closes the
        // WebSocket. The read loop will then observe the close frame and exit.
        drop(self.write_tx);
        // Give the tasks a moment then abort if still running.
        let read_handle = self.read_handle;
        let write_handle = self.write_handle;
        tokio::spawn(async move {
            let timeout = tokio::time::timeout(Duration::from_secs(5), async {
                let _ = write_handle.await;
            });
            if timeout.await.is_err() {
                // Timed out waiting for graceful close.
            }
            read_handle.abort();
        });
    }

    /// Forcefully abort the transport immediately.
    pub fn abort(self) {
        drop(self.write_tx);
        self.read_handle.abort();
        self.write_handle.abort();
    }
}

// ---------------------------------------------------------------------------
// Read loop
// ---------------------------------------------------------------------------

async fn read_loop(
    mut stream: SplitStream<WsStream>,
    event_tx: mpsc::Sender<TransportEvent>,
    format: Format,
) {
    loop {
        match stream.next().await {
            Some(Ok(msg)) => {
                match deserialize_frame(msg, format) {
                    Some(Ok(pm)) => {
                        if event_tx.send(TransportEvent::Message(pm)).await.is_err() {
                            // Receiver dropped — shut down.
                            return;
                        }
                    }
                    Some(Err(e)) => {
                        // Deserialization error — report and continue.
                        let _ = event_tx
                            .send(TransportEvent::Disconnected(Some(e)))
                            .await;
                        return;
                    }
                    None => {
                        // Non-data frame (ping/pong/close) — ignore.
                    }
                }
            }
            Some(Err(e)) => {
                let err = Error::with_cause(
                    ErrorCode::Disconnected,
                    e,
                    "WebSocket read error",
                );
                let _ = event_tx.send(TransportEvent::Disconnected(Some(err))).await;
                return;
            }
            None => {
                // Stream ended.
                let _ = event_tx.send(TransportEvent::Disconnected(None)).await;
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Write loop
// ---------------------------------------------------------------------------

async fn write_loop(
    mut sink: SplitSink<WsStream, tungstenite::Message>,
    mut write_rx: mpsc::Receiver<ProtocolMessage>,
    format: Format,
) {
    while let Some(pm) = write_rx.recv().await {
        let frame = match serialize_frame(&pm, format) {
            Ok(f) => f,
            Err(_) => continue, // Skip messages that fail to serialize.
        };
        if sink.send(frame).await.is_err() {
            // WebSocket write failed — exit.
            return;
        }
    }

    // Channel closed — send a close frame.
    let _ = sink.send(tungstenite::Message::Close(None)).await;
}

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

fn serialize_frame(
    pm: &ProtocolMessage,
    format: Format,
) -> Result<tungstenite::Message> {
    match format {
        Format::JSON => {
            let json = serde_json::to_string(pm)?;
            Ok(tungstenite::Message::Text(json))
        }
        Format::MessagePack => {
            let bytes = rmp_serde::to_vec_named(pm)?;
            Ok(tungstenite::Message::Binary(bytes))
        }
    }
}

fn deserialize_frame(
    msg: tungstenite::Message,
    _format: Format,
) -> Option<Result<ProtocolMessage>> {
    match msg {
        tungstenite::Message::Text(text) => {
            Some(serde_json::from_str::<ProtocolMessage>(&text).map_err(Into::into))
        }
        tungstenite::Message::Binary(bytes) => {
            Some(
                rmp_serde::from_read::<_, ProtocolMessage>(bytes.as_slice())
                    .map_err(Into::into),
            )
        }
        tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => None,
        tungstenite::Message::Close(_) => None,
        tungstenite::Message::Frame(_) => None,
    }
}

// ---------------------------------------------------------------------------
// URL construction
// ---------------------------------------------------------------------------

/// Build the WebSocket URL with auth and protocol params.
fn build_ws_url(opts: &ClientOptions, resume_key: Option<&str>) -> Result<Url> {
    let scheme = if opts.tls { "wss" } else { "ws" };
    let port = if opts.tls { opts.tls_port } else { opts.port };

    let host = match &opts.environment {
        Some(env) => format!("{}-realtime.ably.io", env),
        None => opts.realtime_host.clone(),
    };

    let base = format!("{}://{}:{}/", scheme, host, port);
    let mut url = Url::parse(&base)?;

    {
        let mut q = url.query_pairs_mut();
        q.append_pair("v", &PROTOCOL_VERSION.to_string());

        let format_str = match opts.format {
            Format::JSON => "json",
            Format::MessagePack => "msgpack",
        };
        q.append_pair("format", format_str);
        q.append_pair("heartbeats", "true");
        q.append_pair("echo", "true");

        // API key auth — send the full key as a query param.
        if let auth::Credential::Key(ref key) = opts.credential {
            q.append_pair("key", &format!("{}:{}", key.name, key.value));
        }

        if let Some(client_id) = &opts.client_id {
            q.append_pair("clientId", client_id);
        }

        if let Some(key) = resume_key {
            q.append_pair("resume", key);
        }
    }

    Ok(url)
}
