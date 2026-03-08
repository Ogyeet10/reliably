//! Realtime client — the top-level entry point for Ably Pub/Sub.
//!
//! The [`Realtime`] client manages a persistent WebSocket connection to Ably
//! and provides access to realtime channels for publishing, subscribing, and
//! presence.
//!
//! # Example
//!
//! ```rust,no_run
//! use reliably::realtime::Realtime;
//! use reliably::rest::Data;
//!
//! #[tokio::main]
//! async fn main() -> reliably::Result<()> {
//!     let client = Realtime::new("your-api-key")?;
//!
//!     // Get a channel and subscribe.
//!     let channel = client.channels.get("my-channel").await;
//!     let mut sub = channel.subscribe().await?;
//!
//!     // Publish a message.
//!     channel.publish(Some("greeting"), Data::String("hello".into())).await?;
//!
    //!     // Receive.
    //!     if let Some(msg) = sub.recv().await {
    //!         println!("Received: {:?}", msg.data);
    //!     }
//!
//!     // Close.
//!     client.close().await;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::auth::Auth;
use crate::connection::{Connection, ConnectionManager};
use crate::options::ClientOptions;
use crate::realtime_channel::Channels;
use crate::rest::Rest;
#[allow(unused_imports)]
use crate::Result;

/// A realtime Ably client with a persistent WebSocket connection.
pub struct Realtime {
    /// The connection handle — use this to observe state, connect, close, ping.
    pub connection: Connection,

    /// The channels collection — use this to get/release channels.
    pub channels: Channels,

    /// The underlying REST client (for REST API calls: time, stats, etc.).
    rest: Rest,

    /// Handle to the background manager task.
    _manager_handle: tokio::task::JoinHandle<()>,

    /// Handle to the channel router task.
    _router_handle: tokio::task::JoinHandle<()>,
}

impl Realtime {
    /// Create a new Realtime client from an API key string.
    ///
    /// The client will auto-connect immediately. Use
    /// [`Realtime::from_options`] to configure `auto_connect = false` or
    /// other settings.
    pub fn new(key: &str) -> Result<Self> {
        let opts = ClientOptions::new(key);
        Self::from_options(opts)
    }

    /// Create a new Realtime client from [`ClientOptions`].
    pub fn from_options(opts: ClientOptions) -> Result<Self> {
        // Build the REST client (reuses HTTP client for REST API calls).
        let rest = opts.rest()?;

        // Rebuild ClientOptions for the realtime side (we consumed it for
        // REST). We need the original options for transport configuration.
        // Since ClientOptions doesn't implement Clone, re-create from the
        // REST client's stored options.
        let rt_opts = Arc::new(ClientOptions::from_rest_opts(rest.options()));
        let channel_retry_timeout = rt_opts.channel_retry_timeout;

        // Create connection manager.
        let (connection, channel_msg_rx, manager) = ConnectionManager::new(rt_opts);

        // Create channels collection.
        let channels = Channels::new(connection.clone(), channel_retry_timeout);

        // Spawn the connection manager background task.
        let manager_handle = tokio::spawn(manager.run());

        // Spawn a task to route channel messages and connection state changes.
        let router_handle = {
            let channels_ref = channels.clone_inner();
            let mut channel_msg_rx = channel_msg_rx;
            let mut state_rx = connection.on_state_change();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        msg = channel_msg_rx.recv() => {
                            match msg {
                                Some(pm) => {
                                    channels_ref.process_channel_message(pm).await;
                                }
                                // Sender dropped — connection manager is gone.
                                None => break,
                            }
                        }
                        change = state_rx.recv() => {
                            match change {
                                Ok(change) => {
                                    channels_ref.propagate_connection_state(&change).await;
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    }
                }
            })
        };

        Ok(Self {
            connection,
            channels,
            rest,
            _manager_handle: manager_handle,
            _router_handle: router_handle,
        })
    }

    /// Access the REST auth API (for token management, etc.).
    pub fn auth(&self) -> Auth<'_> {
        self.rest.auth()
    }

    /// Close the realtime connection gracefully.
    pub async fn close(&self) {
        self.connection.close();
    }
}
