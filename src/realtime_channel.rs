//! Realtime channel — attach/detach state machine, publish, and subscribe.
//!
//! Each [`RealtimeChannel`] has its own state machine and manages message
//! subscriptions. Channels talk to the connection manager via the shared
//! [`Connection`] handle and receive inbound protocol messages through an
//! unbounded mpsc channel (no message drops under load).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, watch, RwLock};

use crate::connection::{Connection, ConnectionState, ConnectionStateChange};
use crate::error::{Error, ErrorCode};
use crate::protocol::{
    Action, ProtocolMessage, WireMessage, WirePresenceMessage, flags,
};
use crate::realtime_presence::RealtimePresence;
use crate::rest::{ChannelOptions, Data, Message};
use crate::Result;

// ---------------------------------------------------------------------------
// Channel states
// ---------------------------------------------------------------------------

/// The state of a realtime channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelState {
    Initialized,
    Attaching,
    Attached,
    Detaching,
    Detached,
    Suspended,
    Failed,
}

impl std::fmt::Display for ChannelState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

// ---------------------------------------------------------------------------
// ChannelStateChange
// ---------------------------------------------------------------------------

/// Emitted whenever a channel's state changes.
#[derive(Clone, Debug)]
pub struct ChannelStateChange {
    pub previous: ChannelState,
    pub current: ChannelState,
    pub reason: Option<Error>,
    pub resumed: bool,
}

// ---------------------------------------------------------------------------
// Channel properties
// ---------------------------------------------------------------------------

/// Properties associated with a channel attachment.
#[derive(Clone, Debug, Default)]
pub struct ChannelProperties {
    /// The serial from the last ATTACHED message.
    pub attach_serial: Option<String>,
    /// The latest channel serial (used for resume on re-attach).
    pub channel_serial: Option<String>,
}

// ---------------------------------------------------------------------------
// RealtimeChannel
// ---------------------------------------------------------------------------

/// A realtime channel that supports attach/detach, publish, and subscribe.
pub struct RealtimeChannel {
    /// The channel name.
    pub name: String,

    // Internal state (behind lock for concurrent access)
    inner: Arc<RwLock<ChannelInner>>,

    /// Handle to the connection for sending protocol messages.
    connection: Connection,

    /// Fan-out subscriber list for decoded inbound messages.
    /// Uses unbounded mpsc per subscriber — no message drops under load.
    message_subs: Arc<Mutex<Vec<mpsc::UnboundedSender<Arc<Message>>>>>,

    /// Presence manager for this channel.
    pub presence: RealtimePresence,

    /// Broadcast sender for detailed state changes.
    state_tx: broadcast::Sender<ChannelStateChange>,

    /// Watch channel for the current state (race-free).
    state_watch_tx: Arc<watch::Sender<ChannelState>>,
    state_watch: watch::Receiver<ChannelState>,

    /// Fan-out subscriber list for discontinuity events (non-resumed
    /// re-attach). Applications MUST monitor this for zero-message-loss
    /// guarantees — when fired, messages may have been lost and the app
    /// should backfill from history.
    discontinuity_subs: Arc<Mutex<Vec<mpsc::UnboundedSender<ChannelStateChange>>>>,
}

struct ChannelInner {
    state: ChannelState,
    properties: ChannelProperties,
    options: Option<ChannelOptions>,
    modes: u64,
    /// Whether we were previously attached (for ATTACH_RESUME flag).
    was_attached: bool,
    /// Delay between channel re-attach attempts when suspended.
    channel_retry_timeout: Duration,
}

impl RealtimeChannel {
    /// Create a new channel in the Initialized state.
    pub(crate) fn new(
        name: String,
        connection: Connection,
        options: Option<ChannelOptions>,
        channel_retry_timeout: Duration,
    ) -> Self {
        let (state_tx, _) = broadcast::channel(64);
        let (state_watch_tx, state_watch_rx) = watch::channel(ChannelState::Initialized);

        let presence = RealtimePresence::new(
            name.clone(),
            connection.clone(),
        );

        Self {
            name,
            inner: Arc::new(RwLock::new(ChannelInner {
                state: ChannelState::Initialized,
                properties: ChannelProperties::default(),
                options,
                modes: flags::MODE_ALL,
                was_attached: false,
                channel_retry_timeout,
            })),
            connection,
            message_subs: Arc::new(Mutex::new(Vec::new())),
            presence,
            state_tx,
            state_watch_tx: Arc::new(state_watch_tx),
            state_watch: state_watch_rx,
            discontinuity_subs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Current channel state (non-blocking).
    pub fn state(&self) -> ChannelState {
        *self.state_watch.borrow()
    }

    /// Channel properties (serials).
    pub async fn properties(&self) -> ChannelProperties {
        self.inner.read().await.properties.clone()
    }

    /// Subscribe to detailed channel state changes.
    pub fn on_state_change(&self) -> broadcast::Receiver<ChannelStateChange> {
        self.state_tx.subscribe()
    }

    /// Wait until the channel reaches the given state.
    ///
    /// Returns immediately if already in that state. Race-free.
    pub async fn wait_for_state(&self, target: ChannelState) -> Result<()> {
        let mut rx = self.state_watch.clone();
        rx.wait_for(|s| *s == target)
            .await
            .map(|_| ())
            .map_err(|_| Error::new(ErrorCode::ChannelOperationFailed, "Channel dropped"))
    }

    // -----------------------------------------------------------------------
    // Attach / Detach
    // -----------------------------------------------------------------------

    /// Attach the channel. Resolves when the channel reaches Attached state.
    pub async fn attach(&self) -> Result<()> {
        {
            let inner = self.inner.read().await;
            match inner.state {
                ChannelState::Attached => return Ok(()),
                ChannelState::Failed => {
                    return Err(Error::new(
                        ErrorCode::ChannelOperationFailedInvalidChannelState,
                        "Channel is in failed state",
                    ));
                }
                _ => {}
            }
        }

        // Set state to attaching.
        self.set_state(ChannelState::Attaching, None, false).await;

        // Send ATTACH.
        self.send_attach().await?;

        // Wait for Attached or failure.
        self.wait_for_attach().await
    }

    /// Detach the channel. Resolves when the channel reaches Detached state.
    pub async fn detach(&self) -> Result<()> {
        {
            let inner = self.inner.read().await;
            match inner.state {
                ChannelState::Initialized | ChannelState::Detached => return Ok(()),
                ChannelState::Failed => {
                    return Err(Error::new(
                        ErrorCode::ChannelOperationFailedInvalidChannelState,
                        "Channel is in failed state",
                    ));
                }
                ChannelState::Suspended => {
                    // RTL5j — can detach immediately from suspended.
                    drop(inner);
                    self.set_state(ChannelState::Detached, None, false).await;
                    return Ok(());
                }
                _ => {}
            }
        }

        // Set state to detaching.
        self.set_state(ChannelState::Detaching, None, false).await;

        // Send DETACH.
        self.send_detach().await?;

        // Wait for Detached or failure.
        self.wait_for_detach().await
    }

    // -----------------------------------------------------------------------
    // Publish
    // -----------------------------------------------------------------------

    /// Publish a single message with the given name and data.
    pub async fn publish(
        &self,
        name: Option<&str>,
        data: Data,
    ) -> Result<()> {
        self.publish_messages(vec![Message {
            name: name.map(|s| s.to_string()),
            data,
            ..Message::default()
        }])
        .await
    }

    /// Publish multiple messages.
    pub async fn publish_messages(&self, mut messages: Vec<Message>) -> Result<()> {
        // Auto-attach if needed.
        self.ensure_attached().await?;

        // Encode messages before converting to wire format.
        // This handles JSON → string conversion with encoding set,
        // optional encryption, and base64 for binary data on JSON transport.
        let opts = self.inner.read().await.options.clone();
        let cipher = opts.as_ref().and_then(|o| o.cipher.as_ref());
        for msg in &mut messages {
            msg.encode(&crate::rest::Format::JSON, cipher)?;
        }

        let wire_messages: Vec<WireMessage> = messages
            .iter()
            .map(WireMessage::from_message)
            .collect();

        let mut pm = ProtocolMessage::new(Action::Message);
        pm.channel = Some(self.name.clone());
        pm.messages = Some(wire_messages);

        self.connection.send(pm, true).await
    }

    // -----------------------------------------------------------------------
    // Subscribe
    // -----------------------------------------------------------------------

    /// Subscribe to all messages on this channel.
    ///
    /// Auto-attaches the channel. Returns an unbounded receiver that yields
    /// decoded [`Message`] values. Each subscriber gets its own independent
    /// stream — no message drops regardless of subscriber speed.
    pub async fn subscribe(&self) -> Result<mpsc::UnboundedReceiver<Arc<Message>>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.message_subs.lock().unwrap().push(tx);
        self.ensure_attached().await?;
        Ok(rx)
    }

    /// Subscribe to presence events on this channel.
    ///
    /// Prefer using `channel.presence.subscribe()` directly for more control.
    pub async fn subscribe_presence(
        &self,
    ) -> Result<mpsc::UnboundedReceiver<Arc<WirePresenceMessage>>> {
        self.ensure_attached().await?;
        Ok(self.presence.subscribe())
    }

    /// Subscribe to discontinuity events on this channel.
    ///
    /// A discontinuity occurs when the channel re-attaches without the
    /// RESUMED flag — meaning the server could not guarantee message
    /// continuity. When you receive this event, messages may have been
    /// lost during the gap and you should backfill from the history API.
    ///
    /// **For zero-message-loss systems, you MUST monitor this.**
    pub fn on_discontinuity(&self) -> mpsc::UnboundedReceiver<ChannelStateChange> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.discontinuity_subs.lock().unwrap().push(tx);
        rx
    }

    // -----------------------------------------------------------------------
    // Inbound protocol message processing
    // -----------------------------------------------------------------------

    /// Process an inbound protocol message routed to this channel.
    pub(crate) async fn process_message(&self, pm: ProtocolMessage) {
        let action = match pm.action() {
            Some(a) => a,
            None => return,
        };

        match action {
            Action::Attached => self.on_attached(pm).await,
            Action::Detached => self.on_detached(pm).await,
            Action::Message => self.on_message(pm).await,
            Action::Presence => self.on_presence(pm, false).await,
            Action::Sync => self.on_presence(pm, true).await,
            Action::Error => self.on_error(pm).await,
            _ => {}
        }
    }

    async fn on_attached(&self, pm: ProtocolMessage) {
        let resumed = pm.has_flag(flags::RESUMED);
        let has_presence = pm.has_flag(flags::HAS_PRESENCE);
        let was_already_attached;

        {
            let mut inner = self.inner.write().await;
            was_already_attached = inner.state == ChannelState::Attached;
            // Update attach serial from the ATTACHED message.
            inner.properties.attach_serial = pm.channel_serial.clone();
            // Update channel serial (used for resume on re-attach).
            if pm.channel_serial.is_some() {
                inner.properties.channel_serial = pm.channel_serial.clone();
            }

            if let Some(f) = pm.flags {
                // Extract channel modes from flags.
                inner.modes = f & flags::MODE_ALL;
            }
        }

        if was_already_attached {
            // Already attached — this is a re-attach response.
            if !resumed {
                // RTL18: Continuity lost — presence needs re-sync and we
                // must notify the application of potential message loss.
                self.presence.on_attached(has_presence).await;

                // Emit discontinuity event (RTL18).
                let change = ChannelStateChange {
                    previous: ChannelState::Attached,
                    current: ChannelState::Attached,
                    reason: pm.error.map(|e| e.into_error()),
                    resumed: false,
                };
                self.emit_discontinuity(change);
            }
            // If resumed, nothing to do — silent success.
        } else {
            // Fresh attach (from attaching/suspended/etc).
            self.set_state(ChannelState::Attached, None, resumed).await;
            self.presence.on_attached(has_presence).await;

            // If not resumed on a fresh attach, that's also a discontinuity
            // (channel was detached/suspended, messages were lost).
            if !resumed {
                let change = ChannelStateChange {
                    previous: ChannelState::Attaching,
                    current: ChannelState::Attached,
                    reason: None,
                    resumed: false,
                };
                self.emit_discontinuity(change);
            }
        }
    }

    async fn on_detached(&self, pm: ProtocolMessage) {
        let error = pm.error.map(|e| e.into_error());

        let current_state = self.inner.read().await.state;

        match current_state {
            ChannelState::Detaching => {
                // Expected response to our DETACH request.
                self.set_state(ChannelState::Detached, error, false).await;
            }
            ChannelState::Attaching => {
                // Server rejected the attach — go to suspended with retry.
                self.set_state(ChannelState::Suspended, error, false).await;
                self.schedule_channel_retry().await;
            }
            ChannelState::Attached => {
                // Server-initiated detach — auto re-attach (RTL13a).
                self.set_state(ChannelState::Attaching, error, false).await;
                let _ = self.send_attach().await;
            }
            _ => {
                self.set_state(ChannelState::Detached, error, false).await;
            }
        }
    }

    async fn on_message(&self, pm: ProtocolMessage) {
        // RTL17: Only deliver messages when the channel is attached.
        // Messages arriving in other states are logged and discarded.
        {
            let state = self.inner.read().await.state;
            if state != ChannelState::Attached {
                return;
            }
        }

        // Update channel serial.
        if let Some(serial) = &pm.channel_serial {
            self.inner.write().await.properties.channel_serial = Some(serial.clone());
        }

        if let Some(wire_messages) = pm.messages {
            let parent_id = pm.id.as_deref();
            let parent_conn_id = pm.connection_id.as_deref();
            let parent_ts = pm.timestamp;
            let opts = self.inner.read().await.options.clone();

            for (i, wm) in wire_messages.into_iter().enumerate() {
                let mut msg = wm.into_message(parent_id, parent_conn_id, parent_ts, i);

                // Decode the message (base64, cipher, json, utf-8).
                crate::rest::decode(&mut msg.data, &mut msg.encoding, opts.as_ref());

                // Fan out to all subscribers (unbounded — no drops).
                let msg = Arc::new(msg);
                let mut subs = self.message_subs.lock().unwrap();
                subs.retain(|tx| tx.send(msg.clone()).is_ok());
            }
        }
    }

    async fn on_presence(&self, pm: ProtocolMessage, is_sync: bool) {
        // Update channel serial.
        let channel_serial = pm.channel_serial.clone();
        if let Some(serial) = &channel_serial {
            self.inner.write().await.properties.channel_serial = Some(serial.clone());
        }

        if let Some(presence_messages) = pm.presence {
            if is_sync {
                self.presence
                    .process_sync(presence_messages, channel_serial.as_deref())
                    .await;
            } else {
                self.presence.process_presence(presence_messages, false).await;
            }
        }
    }

    async fn on_error(&self, pm: ProtocolMessage) {
        let error = pm.error.map(|e| e.into_error());
        self.set_state(ChannelState::Failed, error, false).await;
    }

    // -----------------------------------------------------------------------
    // Connection interruption
    // -----------------------------------------------------------------------

    /// Called when the connection state changes to map it to channel state.
    pub(crate) async fn on_connection_state_change(&self, change: &ConnectionStateChange) {
        let current = self.inner.read().await.state;

        // Only propagate to channels in active states.
        if !matches!(
            current,
            ChannelState::Attaching
                | ChannelState::Attached
                | ChannelState::Detaching
                | ChannelState::Suspended
        ) {
            return;
        }

        match change.current {
            ConnectionState::Closing | ConnectionState::Closed => {
                self.set_state(ChannelState::Detached, change.reason.clone(), false)
                    .await;
            }
            ConnectionState::Failed => {
                self.set_state(ChannelState::Failed, change.reason.clone(), false)
                    .await;
            }
            ConnectionState::Suspended => {
                self.set_state(ChannelState::Suspended, change.reason.clone(), false)
                    .await;
            }
            ConnectionState::Connected => {
                // Transport became active — re-attach if needed.
                match current {
                    ChannelState::Attaching | ChannelState::Detaching => {
                        // Re-send the pending operation.
                        if current == ChannelState::Attaching {
                            let _ = self.send_attach().await;
                        } else {
                            let _ = self.send_detach().await;
                        }
                    }
                    ChannelState::Attached => {
                        // Need to re-attach on the new transport.
                        self.set_state(ChannelState::Attaching, None, false).await;
                        let _ = self.send_attach().await;
                    }
                    ChannelState::Suspended => {
                        // Attempt re-attach.
                        self.set_state(ChannelState::Attaching, None, false).await;
                        let _ = self.send_attach().await;
                    }
                    _ => {}
                }
            }
            ConnectionState::Disconnected => {
                // Keep current channel state — connection will retry.
            }
            _ => {}
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    async fn set_state(
        &self,
        new_state: ChannelState,
        reason: Option<Error>,
        resumed: bool,
    ) {
        let previous = {
            let mut inner = self.inner.write().await;
            let prev = inner.state;
            inner.state = new_state;

            // RTP5a1: Clear channelSerial on detached/suspended/failed.
            // Once cleared, re-attach cannot request resume from the
            // server's perspective — continuity is lost.
            if matches!(
                new_state,
                ChannelState::Detached | ChannelState::Suspended | ChannelState::Failed
            ) {
                inner.properties.channel_serial = None;
            }

            // Clear was_attached (ATTACH_RESUME flag) on detach/failed
            // but NOT on suspended (so suspended→attaching still tries resume).
            if matches!(new_state, ChannelState::Detaching | ChannelState::Failed) {
                inner.was_attached = false;
            }

            // Track that we were attached (for future ATTACH_RESUME).
            if new_state == ChannelState::Attached {
                inner.was_attached = true;
            }

            prev
        };

        if previous == new_state {
            return;
        }

        // Notify presence of state changes.
        match new_state {
            ChannelState::Detached | ChannelState::Failed => {
                // Clear all presence state.
                self.presence.clear().await;
            }
            ChannelState::Suspended => {
                // Don't clear members (they remain as stale data),
                // but fail any pending presence operations.
            }
            _ => {}
        }

        // Update watch (race-free for new subscribers).
        let _ = self.state_watch_tx.send(new_state);

        // Broadcast detailed change event.
        let change = ChannelStateChange {
            previous,
            current: new_state,
            reason,
            resumed,
        };
        let _ = self.state_tx.send(change);
    }

    /// Emit a discontinuity event to all subscribers.
    fn emit_discontinuity(&self, change: ChannelStateChange) {
        let mut subs = self.discontinuity_subs.lock().unwrap();
        subs.retain(|tx| tx.send(change.clone()).is_ok());
    }

    async fn send_attach(&self) -> Result<()> {
        let inner = self.inner.read().await;
        let mut pm = ProtocolMessage::new(Action::Attach);
        pm.channel = Some(self.name.clone());

        // Set channel modes.
        pm.set_flag(inner.modes);

        // If we were previously attached, request resume.
        if inner.was_attached {
            pm.set_flag(flags::ATTACH_RESUME);
            pm.channel_serial = inner.properties.channel_serial.clone();
        }

        drop(inner);
        self.connection.send(pm, false).await
    }

    async fn send_detach(&self) -> Result<()> {
        let mut pm = ProtocolMessage::new(Action::Detach);
        pm.channel = Some(self.name.clone());
        self.connection.send(pm, false).await
    }

    async fn ensure_attached(&self) -> Result<()> {
        let state = self.inner.read().await.state;
        match state {
            ChannelState::Attached => Ok(()),
            ChannelState::Initialized
            | ChannelState::Detached
            | ChannelState::Suspended => {
                // Auto-attach.
                self.attach().await
            }
            ChannelState::Attaching => {
                // Wait for it.
                self.wait_for_attach().await
            }
            ChannelState::Failed => Err(Error::new(
                ErrorCode::ChannelOperationFailedInvalidChannelState,
                "Channel is in failed state",
            )),
            ChannelState::Detaching => {
                // Wait for detach then re-attach.
                let _ = self.wait_for_detach().await;
                self.attach().await
            }
        }
    }

    async fn wait_for_attach(&self) -> Result<()> {
        let mut rx = self.state_tx.subscribe();
        let timeout = Duration::from_secs(15);

        let result = tokio::time::timeout(timeout, async {
            loop {
                match rx.recv().await {
                    Ok(change) => match change.current {
                        ChannelState::Attached => return Ok(()),
                        ChannelState::Failed | ChannelState::Suspended => {
                            return Err(change.reason.unwrap_or_else(|| {
                                Error::new(
                                    ErrorCode::ChannelOperationFailed,
                                    format!("Channel entered {} state", change.current),
                                )
                            }));
                        }
                        ChannelState::Detached => {
                            return Err(change.reason.unwrap_or_else(|| {
                                Error::new(
                                    ErrorCode::ChannelOperationFailed,
                                    "Channel detached unexpectedly",
                                )
                            }));
                        }
                        _ => continue,
                    },
                    Err(_) => {
                        return Err(Error::new(
                            ErrorCode::ChannelOperationFailed,
                            "State change channel closed",
                        ));
                    }
                }
            }
        })
        .await;

        match result {
            Ok(r) => r,
            Err(_) => Err(Error::new(
                ErrorCode::ChannelOperationFailedNoResponseFromServer,
                "Attach timed out",
            )),
        }
    }

    async fn wait_for_detach(&self) -> Result<()> {
        let mut rx = self.state_tx.subscribe();
        let timeout = Duration::from_secs(15);

        let result = tokio::time::timeout(timeout, async {
            loop {
                match rx.recv().await {
                    Ok(change) => match change.current {
                        ChannelState::Detached | ChannelState::Failed => return Ok(()),
                        _ => continue,
                    },
                    Err(_) => {
                        return Err(Error::new(
                            ErrorCode::ChannelOperationFailed,
                            "State change channel closed",
                        ));
                    }
                }
            }
        })
        .await;

        match result {
            Ok(r) => r,
            Err(_) => Err(Error::new(
                ErrorCode::ChannelOperationFailedNoResponseFromServer,
                "Detach timed out",
            )),
        }
    }

    async fn schedule_channel_retry(&self) {
        let name = self.name.clone();
        let state_tx = self.state_tx.clone();
        let inner = self.inner.clone();
        let connection = self.connection.clone();
        let delay = self.inner.read().await.channel_retry_timeout;

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let current = inner.read().await.state;
            if current == ChannelState::Suspended {
                // Attempt re-attach.
                let mut inner = inner.write().await;
                inner.state = ChannelState::Attaching;
                drop(inner);

                let _ = state_tx.send(ChannelStateChange {
                    previous: ChannelState::Suspended,
                    current: ChannelState::Attaching,
                    reason: None,
                    resumed: false,
                });

                let mut pm = ProtocolMessage::new(Action::Attach);
                pm.channel = Some(name);
                let _ = connection.send(pm, false).await;
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Channels collection
// ---------------------------------------------------------------------------

/// A thread-safe collection of realtime channels.
///
/// Cheaply cloneable — all clones share the same underlying channel map.
#[derive(Clone)]
pub struct Channels {
    inner: Arc<ChannelsInner>,
}

struct ChannelsInner {
    channels: RwLock<HashMap<String, Arc<RealtimeChannel>>>,
    connection: Connection,
    channel_retry_timeout: Duration,
}

impl Channels {
    pub(crate) fn new(connection: Connection, channel_retry_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(ChannelsInner {
                channels: RwLock::new(HashMap::new()),
                connection,
                channel_retry_timeout,
            }),
        }
    }

    /// Returns a clone that shares the same underlying channel map.
    pub(crate) fn clone_inner(&self) -> Channels {
        self.clone()
    }

    /// Get or create a channel with the given name.
    pub async fn get(&self, name: &str) -> Arc<RealtimeChannel> {
        {
            let channels = self.inner.channels.read().await;
            if let Some(ch) = channels.get(name) {
                return ch.clone();
            }
        }

        let mut channels = self.inner.channels.write().await;
        // Double-check after acquiring write lock.
        if let Some(ch) = channels.get(name) {
            return ch.clone();
        }

        let channel = Arc::new(RealtimeChannel::new(
            name.to_string(),
            self.inner.connection.clone(),
            None,
            self.inner.channel_retry_timeout,
        ));
        channels.insert(name.to_string(), channel.clone());
        channel
    }

    /// Get or create a channel with cipher options.
    pub async fn get_with_options(
        &self,
        name: &str,
        options: ChannelOptions,
    ) -> Arc<RealtimeChannel> {
        let mut channels = self.inner.channels.write().await;

        let channel = Arc::new(RealtimeChannel::new(
            name.to_string(),
            self.inner.connection.clone(),
            Some(options),
            self.inner.channel_retry_timeout,
        ));
        channels.insert(name.to_string(), channel.clone());
        channel
    }

    /// Release a channel. Only possible if the channel is in Initialized,
    /// Detached, or Failed state.
    pub async fn release(&self, name: &str) -> Result<()> {
        let mut channels = self.inner.channels.write().await;
        if let Some(ch) = channels.get(name) {
            let state = ch.state();
            if matches!(
                state,
                ChannelState::Initialized | ChannelState::Detached | ChannelState::Failed
            ) {
                channels.remove(name);
                Ok(())
            } else {
                Err(Error::new(
                    ErrorCode::ChannelOperationFailedInvalidChannelState,
                    format!(
                        "Cannot release channel in {} state; detach first",
                        state
                    ),
                ))
            }
        } else {
            Ok(())
        }
    }

    /// Route an inbound protocol message to the correct channel.
    pub(crate) async fn process_channel_message(&self, pm: ProtocolMessage) {
        let channel_name = match &pm.channel {
            Some(name) => name.clone(),
            None => return,
        };

        let channels = self.inner.channels.read().await;
        if let Some(ch) = channels.get(&channel_name) {
            ch.process_message(pm).await;
        }
        // Messages for unknown channels are silently dropped.
    }

    /// Propagate a connection state change to all channels.
    pub(crate) async fn propagate_connection_state(&self, change: &ConnectionStateChange) {
        let channels = self.inner.channels.read().await;
        for ch in channels.values() {
            ch.on_connection_state_change(change).await;
        }
    }
}
