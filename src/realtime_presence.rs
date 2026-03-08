//! Realtime presence — enter, leave, update, subscribe, and presence sync.
//!
//! [`RealtimePresence`] tracks the set of members currently present on a
//! channel and handles the Ably presence sync protocol.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, RwLock};

use crate::connection::Connection;
use crate::error::{Error, ErrorCode};
use crate::protocol::{Action, ProtocolMessage, WirePresenceMessage};
use crate::rest::{Data, PresenceAction};
use crate::Result;

// ---------------------------------------------------------------------------
// PresenceMap
// ---------------------------------------------------------------------------

/// Tracks presence members for a channel.
///
/// Members are keyed by `"clientId:connectionId"`. During a sync, a snapshot
/// of current members (the "residual" set) is kept so that members who left
/// while the client was disconnected can be detected.
#[derive(Debug)]
pub(crate) struct PresenceMap {
    /// Current members: key = "clientId:connectionId".
    members: HashMap<String, WirePresenceMessage>,
    /// During sync: snapshot of members before sync started.
    residual: Option<HashMap<String, WirePresenceMessage>>,
    /// Whether a sync is in progress.
    sync_in_progress: bool,
    /// Channel serial from the last SYNC message (cursor-based).
    #[allow(dead_code)]
    sync_channel_serial: Option<String>,
}

impl PresenceMap {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            residual: None,
            sync_in_progress: false,
            sync_channel_serial: None,
        }
    }

    /// Start a presence sync.
    pub fn start_sync(&mut self) {
        self.sync_in_progress = true;
        self.residual = Some(self.members.clone());
    }

    /// End a presence sync.
    ///
    /// Returns any members that were in the residual set but not updated
    /// during sync (i.e., they left while we were disconnected).
    pub fn end_sync(&mut self) -> Vec<WirePresenceMessage> {
        self.sync_in_progress = false;
        let mut left = Vec::new();

        if let Some(residual) = self.residual.take() {
            for (key, mut msg) in residual {
                // Members still in residual were not refreshed during sync —
                // they have left.
                if self.members.contains_key(&key) {
                    // Was refreshed — skip.
                    continue;
                }
                msg.action = PresenceAction::Leave;
                left.push(msg);
            }
        }

        // Remove any members marked as absent.
        self.members.retain(|_, m| m.action != PresenceAction::Absent);

        left
    }

    /// Apply a presence message (enter/update/present/leave/absent).
    ///
    /// Returns true if the message was applied (was newer than existing).
    pub fn put(&mut self, msg: WirePresenceMessage) -> bool {
        let key = member_key(&msg.client_id, &msg.connection_id);

        match msg.action {
            PresenceAction::Leave | PresenceAction::Absent => {
                if self.sync_in_progress {
                    // During sync, mark as absent rather than removing.
                    // The end_sync() will clean up.
                    if let Some(existing) = self.members.get(&key) {
                        if !is_newer(&msg, existing) {
                            return false;
                        }
                    }
                    // Remove from residual since we've seen an update.
                    if let Some(ref mut residual) = self.residual {
                        residual.remove(&key);
                    }
                    self.members.remove(&key);
                } else {
                    // Outside sync, just remove.
                    if let Some(existing) = self.members.get(&key) {
                        if !is_newer(&msg, existing) {
                            return false;
                        }
                    }
                    self.members.remove(&key);
                }
                true
            }
            PresenceAction::Enter | PresenceAction::Update | PresenceAction::Present => {
                if let Some(existing) = self.members.get(&key) {
                    if !is_newer(&msg, existing) {
                        return false;
                    }
                }
                // Remove from residual since we've seen an update.
                if let Some(ref mut residual) = self.residual {
                    residual.remove(&key);
                }
                // Store as "present" regardless of whether it was enter/update.
                let mut stored = msg;
                stored.action = PresenceAction::Present;
                self.members.insert(key, stored);
                true
            }
        }
    }

    /// Get all current members.
    pub fn get_members(&self) -> Vec<WirePresenceMessage> {
        self.members.values().cloned().collect()
    }

    /// Clear all members.
    pub fn clear(&mut self) {
        self.members.clear();
        self.residual = None;
        self.sync_in_progress = false;
    }
}

/// Build the member key: "clientId:connectionId".
fn member_key(client_id: &str, connection_id: &str) -> String {
    format!("{}:{}", client_id, connection_id)
}

/// Compare two presence messages for "newness".
///
/// Messages with an `id` in the format `"connectionId:msgSerial:index"` are
/// compared by `(msgSerial, index)`. Otherwise comparison falls back to
/// timestamp, with a bias toward the newer message.
fn is_newer(incoming: &WirePresenceMessage, existing: &WirePresenceMessage) -> bool {
    // Try to parse ids as connectionId:serial:index.
    if let (Some(ref inc_id), Some(ref ex_id)) = (&incoming.id, &existing.id) {
        if let (Some(inc_parsed), Some(ex_parsed)) = (parse_msg_id(inc_id), parse_msg_id(ex_id)) {
            // Same connection — compare serial then index.
            if inc_parsed.0 == ex_parsed.0 {
                return (inc_parsed.1, inc_parsed.2) > (ex_parsed.1, ex_parsed.2);
            }
        }
    }

    // Fallback: compare by timestamp (incoming >= existing means newer).
    match (incoming.timestamp, existing.timestamp) {
        (Some(inc_ts), Some(ex_ts)) => inc_ts >= ex_ts,
        _ => true, // Assume newer if timestamps unavailable.
    }
}

/// Parse a message id of the form "connectionId:serial:index" into
/// (connectionId, serial, index).
fn parse_msg_id(id: &str) -> Option<(&str, u64, u64)> {
    let parts: Vec<&str> = id.splitn(3, ':').collect();
    if parts.len() == 3 {
        let serial = parts[1].parse().ok()?;
        let index = parts[2].parse().ok()?;
        Some((parts[0], serial, index))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// RealtimePresence
// ---------------------------------------------------------------------------

/// Manages presence for a single realtime channel.
pub struct RealtimePresence {
    /// Channel name (for building protocol messages).
    channel_name: String,
    /// All presence members on the channel.
    members: Arc<RwLock<PresenceMap>>,
    /// Our own presence entries (keyed by clientId only).
    my_members: Arc<RwLock<HashMap<String, WirePresenceMessage>>>,
    /// Fan-out subscriber list for presence events (unbounded — no drops).
    event_subs: Arc<Mutex<Vec<mpsc::UnboundedSender<Arc<WirePresenceMessage>>>>>,
    /// Connection handle for sending presence messages.
    connection: Connection,
}

impl RealtimePresence {
    pub(crate) fn new(
        channel_name: String,
        connection: Connection,
    ) -> Self {
        Self {
            channel_name,
            members: Arc::new(RwLock::new(PresenceMap::new())),
            my_members: Arc::new(RwLock::new(HashMap::new())),
            event_subs: Arc::new(Mutex::new(Vec::new())),
            connection,
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Enter presence on the channel.
    pub async fn enter(&self, data: Option<Data>) -> Result<()> {
        let client_id = self.require_client_id().await?;
        self.send_presence(PresenceAction::Enter, client_id.clone(), data.clone())
            .await?;

        // Track in my_members.
        let msg = WirePresenceMessage::new(
            PresenceAction::Present,
            client_id.clone(),
            data.unwrap_or(Data::None),
        );
        self.my_members.write().await.insert(client_id, msg);

        Ok(())
    }

    /// Leave presence on the channel.
    pub async fn leave(&self, data: Option<Data>) -> Result<()> {
        let client_id = self.require_client_id().await?;
        self.send_presence(PresenceAction::Leave, client_id.clone(), data)
            .await?;

        // Remove from my_members.
        self.my_members.write().await.remove(&client_id);

        Ok(())
    }

    /// Update presence data on the channel.
    pub async fn update(&self, data: Option<Data>) -> Result<()> {
        let client_id = self.require_client_id().await?;
        self.send_presence(PresenceAction::Update, client_id.clone(), data.clone())
            .await?;

        // Update in my_members.
        let msg = WirePresenceMessage::new(
            PresenceAction::Present,
            client_id.clone(),
            data.unwrap_or(Data::None),
        );
        self.my_members.write().await.insert(client_id, msg);

        Ok(())
    }

    /// Get all current presence members.
    pub async fn get(&self) -> Vec<WirePresenceMessage> {
        self.members.read().await.get_members()
    }

    /// Subscribe to presence events.
    ///
    /// Each subscriber gets an independent unbounded stream — no drops.
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<Arc<WirePresenceMessage>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.event_subs.lock().unwrap().push(tx);
        rx
    }

    /// Fan out a presence event to all subscribers.
    fn emit_event(&self, msg: Arc<WirePresenceMessage>) {
        let mut subs = self.event_subs.lock().unwrap();
        subs.retain(|tx| tx.send(msg.clone()).is_ok());
    }

    // -----------------------------------------------------------------------
    // Channel state transitions
    // -----------------------------------------------------------------------

    /// Called when the channel reaches Attached state (fresh or re-attach).
    ///
    /// If `has_presence` is true, starts a sync — SYNC messages will follow.
    /// If false, synthesizes leave events for any existing members and clears.
    /// In both cases, re-enters own presence members (RTP17i).
    pub(crate) async fn on_attached(&self, has_presence: bool) {
        if has_presence {
            self.members.write().await.start_sync();
        } else {
            // No presence on this channel — synthesize leave events for
            // any members we had.
            let existing = self.members.read().await.get_members();
            for mut msg in existing {
                msg.action = PresenceAction::Leave;
                self.emit_event(Arc::new(msg));
            }
            self.members.write().await.clear();
        }

        // RTP17i: Re-enter own members on the new attachment.
        let _ = self.reenter_own_members().await;
    }

    // -----------------------------------------------------------------------
    // Inbound presence processing
    // -----------------------------------------------------------------------

    /// Process inbound presence messages (from PRESENCE or SYNC actions).
    pub(crate) async fn process_presence(
        &self,
        messages: Vec<WirePresenceMessage>,
        _is_sync: bool,
    ) {
        let mut members = self.members.write().await;

        for msg in messages {
            if members.put(msg.clone()) {
                // Fan out the event to all subscribers.
                self.emit_event(Arc::new(msg));
            }
        }
    }

    /// Start a presence sync (called when ATTACHED has HAS_PRESENCE flag).
    #[allow(dead_code)]
    pub(crate) async fn start_sync(&self) {
        self.members.write().await.start_sync();
    }

    /// Process a SYNC message.
    pub(crate) async fn process_sync(
        &self,
        messages: Vec<WirePresenceMessage>,
        channel_serial: Option<&str>,
    ) {
        // Apply all presence updates.
        self.process_presence(messages, true).await;

        // Check if sync is complete: the channel_serial cursor part is empty.
        let sync_complete = match channel_serial {
            Some(serial) => {
                // Format: "connectionId:cursor" — if cursor is empty, sync done.
                match serial.split_once(':') {
                    Some((_, cursor)) => cursor.is_empty(),
                    None => true,
                }
            }
            None => true,
        };

        if sync_complete {
            let left_members = self.members.write().await.end_sync();

            // Fan out LEAVE events for members who left during disconnect.
            for msg in left_members {
                self.emit_event(Arc::new(msg));
            }
        }
    }

    /// Re-enter all own presence members (after non-resumed re-attach).
    pub(crate) async fn reenter_own_members(&self) -> Result<()> {
        let my = self.my_members.read().await.clone();

        for (_, member) in my {
            let result = self
                .send_presence(
                    PresenceAction::Enter,
                    member.client_id.clone(),
                    if member.data.is_none() {
                        None
                    } else {
                        Some(member.data.clone())
                    },
                )
                .await;

            if let Err(e) = result {
                // Log the error but don't fail — matches JS SDK behavior
                // (emits channel update event with error 91004).
                eprintln!(
                    "Failed to re-enter presence for {}: {}",
                    member.client_id, e
                );
            }
        }

        Ok(())
    }

    /// Clear all presence state (on channel detach/fail).
    pub(crate) async fn clear(&self) {
        self.members.write().await.clear();
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    async fn require_client_id(&self) -> Result<String> {
        let shared = self.connection.shared.read().await;
        if let Some(ref client_id) = shared.client_id {
            Ok(client_id.clone())
        } else {
            Err(Error::new(
                ErrorCode::UnableToEnterPresenceChannelNoClientID,
                "No clientId configured; set client_id in ClientOptions for presence",
            ))
        }
    }

    async fn send_presence(
        &self,
        action: PresenceAction,
        client_id: String,
        data: Option<Data>,
    ) -> Result<()> {
        let wpm = WirePresenceMessage::new(
            action,
            client_id,
            data.unwrap_or(Data::None),
        );

        let mut pm = ProtocolMessage::new(Action::Presence);
        pm.channel = Some(self.channel_name.clone());
        pm.presence = Some(vec![wpm]);

        self.connection.send(pm, true).await
    }
}
