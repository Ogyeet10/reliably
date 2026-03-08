//! Ably realtime protocol types.
//!
//! Defines the wire format for communication between client and server over
//! WebSocket, including protocol message actions, flags, and the
//! [`ProtocolMessage`] envelope.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::error::{Error, ErrorCode};
use crate::rest::{Data, Encoding, Message, PresenceAction, PresenceMessage};
use crate::Result;

// ---------------------------------------------------------------------------
// Protocol version
// ---------------------------------------------------------------------------

/// The Ably protocol version spoken by this client.
pub const PROTOCOL_VERSION: u8 = 5;

// ---------------------------------------------------------------------------
// Action codes
// ---------------------------------------------------------------------------

/// Actions that can appear in a [`ProtocolMessage`].
///
/// Integer values match the Ably wire protocol (protocol version 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum Action {
    Heartbeat = 0,
    Ack = 1,
    Nack = 2,
    Connect = 3,
    Connected = 4,
    Disconnect = 5,
    Disconnected = 6,
    Close = 7,
    Closed = 8,
    Error = 9,
    Attach = 10,
    Attached = 11,
    Detach = 12,
    Detached = 13,
    Presence = 14,
    Message = 15,
    Sync = 16,
    Auth = 17,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// ---------------------------------------------------------------------------
// Protocol flags (bitmask)
// ---------------------------------------------------------------------------

/// Bitmask flags used in the `flags` field of a [`ProtocolMessage`].
pub mod flags {
    /// ATTACHED: the channel has presence members.
    pub const HAS_PRESENCE: u64 = 1 << 0;
    /// ATTACHED: the channel has a backlog of messages.
    pub const HAS_BACKLOG: u64 = 1 << 1;
    /// ATTACHED/CONNECTED: the channel/connection was resumed.
    pub const RESUMED: u64 = 1 << 2;
    /// ATTACH: request resume from a previous attachment.
    pub const ATTACH_RESUME: u64 = 1 << 5;

    // Channel mode flags (used in ATTACH / ATTACHED)
    /// Channel mode: presence operations allowed.
    pub const PRESENCE: u64 = 1 << 16;
    /// Channel mode: publish allowed.
    pub const PUBLISH: u64 = 1 << 17;
    /// Channel mode: subscribe allowed.
    pub const SUBSCRIBE: u64 = 1 << 18;
    /// Channel mode: presence subscribe allowed.
    pub const PRESENCE_SUBSCRIBE: u64 = 1 << 19;

    /// All channel mode flags OR'd together.
    pub const MODE_ALL: u64 = PRESENCE | PUBLISH | SUBSCRIBE | PRESENCE_SUBSCRIBE;

    /// Check whether a flag is set in the given bitmask.
    #[inline]
    pub fn has(value: u64, flag: u64) -> bool {
        value & flag != 0
    }
}

// ---------------------------------------------------------------------------
// ConnectionDetails
// ---------------------------------------------------------------------------

/// Server-provided connection metadata, received in CONNECTED messages.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_idle_interval: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_state_ttl: Option<u64>,
}

// ---------------------------------------------------------------------------
// ErrorInfo (lightweight, for embedding in ProtocolMessage)
// ---------------------------------------------------------------------------

/// A lightweight error payload embedded in protocol messages.
///
/// Distinct from [`crate::error::Error`] which is the richer client-side type.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorInfo {
    #[serde(default)]
    pub code: u32,
    #[serde(default)]
    pub status_code: u32,
    #[serde(default)]
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,
}

impl ErrorInfo {
    /// Convert into the crate-level [`Error`] type.
    pub fn into_error(self) -> Error {
        let code = ErrorCode::new(self.code).unwrap_or(ErrorCode::UnknownError);
        Error::with_status(code, self.status_code, self.message)
    }
}

impl From<ErrorInfo> for Error {
    fn from(e: ErrorInfo) -> Self {
        e.into_error()
    }
}

// ---------------------------------------------------------------------------
// ProtocolMessage
// ---------------------------------------------------------------------------

/// The envelope for all communication on an Ably realtime connection.
///
/// Every WebSocket frame (text for JSON, binary for msgpack) carries exactly
/// one `ProtocolMessage`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolMessage {
    pub action: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub flags: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorInfo>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_serial: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_serial: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<WireMessage>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence: Option<Vec<WirePresenceMessage>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthPayload>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_details: Option<ConnectionDetails>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,
}

impl ProtocolMessage {
    /// Create a new ProtocolMessage with the given action code.
    pub fn new(action: Action) -> Self {
        Self {
            action: Some(action as u8),
            ..Default::default()
        }
    }

    /// Parse the `action` field into a typed [`Action`], if valid.
    pub fn action(&self) -> Option<Action> {
        self.action.and_then(action_from_u8)
    }

    /// Check whether a flag is set.
    pub fn has_flag(&self, flag: u64) -> bool {
        self.flags.map_or(false, |f| flags::has(f, flag))
    }

    /// Set a flag in the bitmask.
    pub fn set_flag(&mut self, flag: u64) {
        let current = self.flags.unwrap_or(0);
        self.flags = Some(current | flag);
    }

    /// Returns true if this action type requires an ACK from the server.
    pub fn ack_required(&self) -> bool {
        matches!(
            self.action(),
            Some(Action::Message) | Some(Action::Presence)
        )
    }
}

/// Convert a raw u8 into an [`Action`].
fn action_from_u8(v: u8) -> Option<Action> {
    match v {
        0 => Some(Action::Heartbeat),
        1 => Some(Action::Ack),
        2 => Some(Action::Nack),
        3 => Some(Action::Connect),
        4 => Some(Action::Connected),
        5 => Some(Action::Disconnect),
        6 => Some(Action::Disconnected),
        7 => Some(Action::Close),
        8 => Some(Action::Closed),
        9 => Some(Action::Error),
        10 => Some(Action::Attach),
        11 => Some(Action::Attached),
        12 => Some(Action::Detach),
        13 => Some(Action::Detached),
        14 => Some(Action::Presence),
        15 => Some(Action::Message),
        16 => Some(Action::Sync),
        17 => Some(Action::Auth),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Wire message types (sub-messages inside a ProtocolMessage)
// ---------------------------------------------------------------------------

/// A message as it appears on the wire inside a [`ProtocolMessage`].
///
/// Fields mirror the REST [`Message`] but the serde naming follows the Ably
/// wire protocol (`camelCase`).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WireMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(default, skip_serializing_if = "Data::is_none")]
    pub data: Data,
    #[serde(default, skip_serializing_if = "Encoding::is_none")]
    pub encoding: Encoding,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extras: Option<crate::json::Map>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
}

impl WireMessage {
    /// Convert a REST [`Message`] into a wire message for sending.
    pub fn from_message(msg: &Message) -> Self {
        Self {
            id: msg.id.clone(),
            name: msg.name.clone(),
            client_id: msg.client_id.clone(),
            connection_id: msg.connection_id.clone(),
            data: msg.data.clone(),
            encoding: match &msg.encoding {
                Encoding::None => Encoding::None,
                Encoding::Some(s) => Encoding::Some(s.clone()),
            },
            extras: msg.extras.clone(),
            timestamp: None,
        }
    }

    /// Convert into a REST [`Message`], populating parent fields from the
    /// protocol message envelope.
    pub fn into_message(
        self,
        parent_id: Option<&str>,
        parent_connection_id: Option<&str>,
        _parent_timestamp: Option<i64>,
        index: usize,
    ) -> Message {
        let id = self
            .id
            .or_else(|| parent_id.map(|pid| format!("{}:{}", pid, index)));
        let connection_id = self
            .connection_id
            .or_else(|| parent_connection_id.map(|s| s.to_string()));
        Message {
            id,
            name: self.name,
            data: self.data,
            encoding: self.encoding,
            client_id: self.client_id,
            connection_id,
            extras: self.extras,
        }
    }
}

/// A presence message as it appears on the wire.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WirePresenceMessage {
    pub action: PresenceAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub client_id: String,
    #[serde(default)]
    pub connection_id: String,
    #[serde(default, skip_serializing_if = "Data::is_none")]
    pub data: Data,
    #[serde(default, skip_serializing_if = "Encoding::is_none")]
    pub encoding: Encoding,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
}

impl WirePresenceMessage {
    /// Convert into a REST [`PresenceMessage`].
    pub fn into_presence_message(self) -> PresenceMessage {
        PresenceMessage {
            action: self.action,
            client_id: self.client_id,
            connection_id: self.connection_id,
            data: self.data,
            encoding: self.encoding,
        }
    }

    /// Build a wire presence message for sending.
    pub fn new(action: PresenceAction, client_id: String, data: Data) -> Self {
        Self {
            action,
            id: None,
            client_id,
            connection_id: String::new(),
            data,
            encoding: Encoding::None,
            timestamp: None,
        }
    }
}

/// Auth payload embedded in an AUTH protocol message.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthPayload {
    pub access_token: String,
}

// ---------------------------------------------------------------------------
// MessageQueue — tracks in-flight messages awaiting ACK/NACK
// ---------------------------------------------------------------------------

/// A message waiting for an ACK or NACK from the server.
pub(crate) struct PendingMessage {
    pub protocol_message: ProtocolMessage,
    pub callback: tokio::sync::oneshot::Sender<Result<()>>,
    pub send_attempted: bool,
}

/// Queue of messages that have been sent and are awaiting ACK/NACK.
///
/// Messages are keyed by their `msg_serial` which is assigned sequentially.
pub(crate) struct MessageQueue {
    queue: VecDeque<PendingMessage>,
}

impl MessageQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    /// Push a pending message onto the queue.
    pub fn push(&mut self, msg: PendingMessage) {
        self.queue.push_back(msg);
    }

    /// Complete `count` messages starting from `serial`.
    ///
    /// If `error` is `Some`, the messages are completed with that error (NACK).
    /// If `error` is `None`, the messages are completed successfully (ACK).
    pub fn complete(&mut self, _serial: u64, count: u64, error: Option<Error>) {
        for _ in 0..count {
            if let Some(pending) = self.queue.pop_front() {
                let result = match &error {
                    Some(e) => Err(Error::new(e.code, e.message.clone())),
                    None => Ok(()),
                };
                // Ignore send errors — the receiver may have been dropped
                // (e.g. the caller timed out).
                let _ = pending.callback.send(result);
            }
        }
    }

    /// Fail all queued messages with the given error.
    pub fn fail_all(&mut self, error: &Error) {
        while let Some(pending) = self.queue.pop_front() {
            let _ = pending
                .callback
                .send(Err(Error::new(error.code, error.message.clone())));
        }
    }

    /// Drain all pending messages back into a `VecDeque` for re-queuing
    /// after a transport drop.
    pub fn drain_to(&mut self, target: &mut VecDeque<PendingMessage>) {
        while let Some(mut pending) = self.queue.pop_front() {
            pending.send_attempted = true;
            target.push_back(pending);
        }
    }

    /// Returns the number of messages in the queue.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the queue is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}
