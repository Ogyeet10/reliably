//! Connection state machine and manager for the Ably realtime protocol.
//!
//! The [`ConnectionManager`] runs as a background tokio task, owns the
//! transport, and processes inbound protocol messages, user commands, and
//! timer expirations. The public [`Connection`] handle provides a thin,
//! cloneable API for interacting with the manager.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc, oneshot, watch, RwLock};

use crate::error::Error;
use crate::error::ErrorCode;
use crate::options::ClientOptions;
use crate::protocol::{
    Action, ConnectionDetails, MessageQueue, PendingMessage, ProtocolMessage,
    flags,
};
use crate::transport::{Transport, TransportEvent};
use crate::Result;

// ---------------------------------------------------------------------------
// Connection states
// ---------------------------------------------------------------------------

/// The state of the realtime connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Initialized,
    Connecting,
    Connected,
    Disconnected,
    Suspended,
    Closing,
    Closed,
    Failed,
}

impl ConnectionState {
    /// Whether messages should be queued locally in this state.
    pub fn queue_messages(self) -> bool {
        matches!(
            self,
            Self::Initialized | Self::Connecting | Self::Disconnected
        )
    }

    /// Whether messages can be sent immediately in this state.
    pub fn send_messages(self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Whether this is a terminal state (no further transitions).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Failed)
    }

    /// The state to transition to when a connecting/closing timer expires.
    pub fn fail_state(self) -> Self {
        match self {
            Self::Connecting | Self::Connected | Self::Disconnected => Self::Disconnected,
            Self::Suspended => Self::Suspended,
            Self::Closing => Self::Closed,
            _ => self,
        }
    }
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

// ---------------------------------------------------------------------------
// ConnectionStateChange
// ---------------------------------------------------------------------------

/// Emitted whenever the connection state changes.
#[derive(Clone, Debug)]
pub struct ConnectionStateChange {
    pub previous: ConnectionState,
    pub current: ConnectionState,
    pub reason: Option<Error>,
    pub retry_in: Option<Duration>,
}



// ---------------------------------------------------------------------------
// Commands from the public Connection handle to the manager
// ---------------------------------------------------------------------------

/// Commands sent from the public [`Connection`] to the [`ConnectionManager`].
pub(crate) enum Command {
    /// Request to connect (or reconnect).
    Connect,
    /// Request to close the connection.
    Close,
    /// Send a protocol message (with optional ACK callback).
    Send {
        msg: ProtocolMessage,
        cb: Option<oneshot::Sender<Result<()>>>,
    },
    /// Ping the server (returns round-trip duration).
    Ping(oneshot::Sender<Result<Duration>>),
}

// ---------------------------------------------------------------------------
// Shared state (readable by Connection handle)
// ---------------------------------------------------------------------------

/// Shared state that the [`Connection`] handle can read without going
/// through the manager task.
pub(crate) struct SharedState {
    pub state: ConnectionState,
    pub connection_id: Option<String>,
    pub connection_key: Option<String>,
    /// The client ID — set from ClientOptions initially, may be updated by
    /// the server in ConnectionDetails on CONNECTED.
    pub client_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Public Connection handle
// ---------------------------------------------------------------------------

/// A handle to the realtime connection.
///
/// Cheaply cloneable — all clones refer to the same underlying connection.
#[derive(Clone)]
pub struct Connection {
    pub(crate) cmd_tx: mpsc::Sender<Command>,
    pub(crate) shared: Arc<RwLock<SharedState>>,
    pub(crate) state_tx: broadcast::Sender<ConnectionStateChange>,
    /// Watch channel for the current connection state. Unlike broadcast,
    /// watch always retains the latest value so new subscribers immediately
    /// see the current state — no race conditions.
    pub(crate) state_watch: watch::Receiver<ConnectionState>,
}

impl Connection {
    /// Current connection state (non-blocking).
    pub fn state(&self) -> ConnectionState {
        *self.state_watch.borrow()
    }

    /// The server-assigned connection ID, if connected.
    pub async fn id(&self) -> Option<String> {
        self.shared.read().await.connection_id.clone()
    }

    /// The connection key used for resumption, if available.
    pub async fn key(&self) -> Option<String> {
        self.shared.read().await.connection_key.clone()
    }

    /// Subscribe to connection state changes (detailed events with
    /// previous state, reason, etc.).
    pub fn on_state_change(&self) -> broadcast::Receiver<ConnectionStateChange> {
        self.state_tx.subscribe()
    }

    /// Wait until the connection reaches the given state.
    ///
    /// Returns immediately if the connection is already in that state.
    /// This is race-free — even if the state changed before this method
    /// was called, it will see it.
    pub async fn wait_for_state(&self, target: ConnectionState) -> Result<()> {
        let mut rx = self.state_watch.clone();
        // wait_for returns immediately if the current value matches.
        rx.wait_for(|s| *s == target)
            .await
            .map(|_| ())
            .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "Connection manager gone"))
    }

    /// Wait until the connection reaches the given state, with a timeout.
    pub async fn wait_for_state_with_timeout(
        &self,
        target: ConnectionState,
        timeout: Duration,
    ) -> Result<()> {
        tokio::time::timeout(timeout, self.wait_for_state(target))
            .await
            .map_err(|_| {
                Error::new(
                    ErrorCode::ConnectionTimedOut,
                    format!("Timed out waiting for {:?} state", target),
                )
            })?
    }

    /// Request to connect (or reconnect from closed/failed).
    pub fn connect(&self) {
        let _ = self.cmd_tx.try_send(Command::Connect);
    }

    /// Request a graceful close.
    pub fn close(&self) {
        let _ = self.cmd_tx.try_send(Command::Close);
    }

    /// Ping the server and return the round-trip duration.
    pub async fn ping(&self) -> Result<Duration> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Ping(tx))
            .await
            .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "Connection manager gone"))?;
        rx.await
            .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "Ping response lost"))?
    }

    /// Send a protocol message through the connection.
    ///
    /// If the message requires an ACK, the returned future resolves when the
    /// ACK is received. For fire-and-forget messages (ATTACH, DETACH, etc.)
    /// the future resolves once the message is queued/sent.
    pub(crate) async fn send(&self, msg: ProtocolMessage, await_ack: bool) -> Result<()> {
        if await_ack {
            let (tx, rx) = oneshot::channel();
            self.cmd_tx
                .send(Command::Send {
                    msg,
                    cb: Some(tx),
                })
                .await
                .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "Connection manager gone"))?;
            rx.await
                .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "ACK response lost"))?
        } else {
            self.cmd_tx
                .send(Command::Send { msg, cb: None })
                .await
                .map_err(|_| Error::new(ErrorCode::ConnectionFailed, "Connection manager gone"))?;
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectionManager
// ---------------------------------------------------------------------------

/// The connection manager runs as a single background task and owns all
/// mutable connection state.
pub(crate) struct ConnectionManager {
    // Configuration
    opts: Arc<ClientOptions>,

    // Current state
    state: ConnectionState,
    connection_id: Option<String>,
    connection_key: Option<String>,
    connection_details: Option<ConnectionDetails>,
    connection_state_ttl: Duration,

    // Serial for outgoing messages
    msg_serial: u64,

    // Transport
    transport: Option<Transport>,
    transport_event_tx: mpsc::Sender<TransportEvent>,
    transport_event_rx: mpsc::Receiver<TransportEvent>,

    // Message queuing
    message_queue: MessageQueue,
    queued_messages: VecDeque<PendingMessage>,

    // Commands from Connection handle
    cmd_rx: mpsc::Receiver<Command>,

    // Shared state for reads
    shared: Arc<RwLock<SharedState>>,

    // Broadcast state changes
    state_tx: broadcast::Sender<ConnectionStateChange>,

    // Channel message routing (unbounded so we never drop protocol messages)
    channel_msg_tx: mpsc::UnboundedSender<ProtocolMessage>,

    // Ping tracking
    pending_pings: Vec<(String, Instant, oneshot::Sender<Result<Duration>>)>,

    // Watch channel for state (sender side)
    state_watch_tx: watch::Sender<ConnectionState>,

    // Timer state
    retry_count: u32,
    suspend_start: Option<Instant>,

    // Idle/heartbeat timeout
    /// Effective max idle interval = server's maxIdleInterval + request timeout.
    /// None means the server didn't provide one (no idle check).
    max_idle_interval: Option<Duration>,
    /// Timestamp of last activity (any inbound protocol message).
    last_activity: Instant,
}

impl ConnectionManager {
    /// Create a new connection manager and return the public handles.
    ///
    /// The manager task is NOT started yet — call [`Self::run`] to start it.
    pub fn new(
        opts: Arc<ClientOptions>,
    ) -> (
        Connection,
        mpsc::UnboundedReceiver<ProtocolMessage>,
        ConnectionManager,
    ) {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let (state_tx, _) = broadcast::channel(64);
        let (channel_msg_tx, channel_msg_rx) = mpsc::unbounded_channel();
        let (transport_event_tx, transport_event_rx) = mpsc::channel(128);
        let (state_watch_tx, state_watch_rx) = watch::channel(ConnectionState::Initialized);

        let shared = Arc::new(RwLock::new(SharedState {
            state: ConnectionState::Initialized,
            connection_id: None,
            connection_key: None,
            client_id: opts.client_id.clone(),
        }));

        let connection = Connection {
            cmd_tx,
            shared: shared.clone(),
            state_tx: state_tx.clone(),
            state_watch: state_watch_rx,
        };

        let manager = ConnectionManager {
            opts,
            state: ConnectionState::Initialized,
            connection_id: None,
            connection_key: None,
            connection_details: None,
            connection_state_ttl: Duration::from_secs(120),
            msg_serial: 0,
            transport: None,
            transport_event_tx,
            transport_event_rx,
            message_queue: MessageQueue::new(),
            queued_messages: VecDeque::new(),
            cmd_rx,
            shared,
            state_tx,
            channel_msg_tx,
            pending_pings: Vec::new(),
            state_watch_tx,
            retry_count: 0,
            suspend_start: None,
            max_idle_interval: None,
            last_activity: Instant::now(),
        };

        (connection, channel_msg_rx, manager)
    }

    /// Run the connection manager event loop.
    ///
    /// This should be spawned as a tokio task. It runs until the connection
    /// enters a terminal state and all commands have been processed.
    pub async fn run(mut self) {
        // If auto_connect is set, start connecting immediately.
        if self.opts.auto_connect {
            self.start_connect().await;
        }

        loop {
            // If we're in a terminal state and have no pending work, exit.
            if self.state.is_terminal() && self.cmd_rx.is_closed() {
                break;
            }

            // Compute the idle timeout deadline.
            let idle_deadline = match self.max_idle_interval {
                Some(interval) if self.state == ConnectionState::Connected => {
                    self.last_activity + interval
                }
                _ => {
                    // Idle check disabled — sleep far into the future.
                    Instant::now() + Duration::from_secs(86400)
                }
            };

            tokio::select! {
                // Process commands from the Connection handle.
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(Command::Connect) => self.handle_connect().await,
                        Some(Command::Close) => self.handle_close().await,
                        Some(Command::Send { msg, cb }) => self.handle_send(msg, cb).await,
                        Some(Command::Ping(tx)) => self.handle_ping(tx).await,
                        None => {
                            // All Connection handles dropped — close.
                            if !self.state.is_terminal() {
                                self.handle_close().await;
                            }
                            break;
                        }
                    }
                }

                // Process transport events.
                event = self.transport_event_rx.recv() => {
                    match event {
                        Some(TransportEvent::Message(pm)) => {
                            self.handle_protocol_message(pm).await;
                        }
                        Some(TransportEvent::Disconnected(err)) => {
                            self.handle_transport_disconnected(err).await;
                        }
                        None => {
                            // Transport event channel closed — should not
                            // happen while manager is alive, but handle
                            // gracefully.
                        }
                    }
                }

                // Idle/heartbeat timeout — no activity from server.
                _ = tokio::time::sleep_until(idle_deadline.into()) => {
                    self.handle_idle_timeout().await;
                }
            }
        }

        // Clean up.
        if let Some(transport) = self.transport.take() {
            transport.abort();
        }
        let err = Error::new(ErrorCode::ConnectionClosed, "Connection closed");
        self.message_queue.fail_all(&err);
        self.fail_queued_messages(&err);
    }

    // -----------------------------------------------------------------------
    // State transitions
    // -----------------------------------------------------------------------

    async fn set_state(&mut self, new_state: ConnectionState, reason: Option<Error>, retry_in: Option<Duration>) {
        let previous = self.state;
        if previous == new_state {
            return;
        }

        self.state = new_state;

        // Update shared state.
        {
            let mut shared = self.shared.write().await;
            shared.state = new_state;
            shared.connection_id = self.connection_id.clone();
            shared.connection_key = self.connection_key.clone();
        }

        // Update watch channel (always retains latest value for new subscribers).
        let _ = self.state_watch_tx.send(new_state);

        // Broadcast the detailed state change event.
        let change = ConnectionStateChange {
            previous,
            current: new_state,
            reason,
            retry_in,
        };
        let _ = self.state_tx.send(change);
    }

    // -----------------------------------------------------------------------
    // Command handlers
    // -----------------------------------------------------------------------

    async fn handle_connect(&mut self) {
        match self.state {
            ConnectionState::Initialized
            | ConnectionState::Closed
            | ConnectionState::Failed
            | ConnectionState::Disconnected => {
                self.start_connect().await;
            }
            _ => {
                // Already connecting or connected — ignore.
            }
        }
    }

    async fn handle_close(&mut self) {
        match self.state {
            ConnectionState::Connected => {
                self.set_state(ConnectionState::Closing, None, None).await;

                // Send CLOSE protocol message.
                let close_msg = ProtocolMessage::new(Action::Close);
                if let Some(transport) = &self.transport {
                    let _ = transport.send(close_msg).await;
                }

                // Start a timeout — if we don't receive CLOSED, force it.
                let shared = self.shared.clone();
                let transport_event_tx = self.transport_event_tx.clone();
                let timeout = self.opts.http_request_timeout;
                tokio::spawn(async move {
                    tokio::time::sleep(timeout).await;
                    // If still closing, force disconnect.
                    let s = shared.read().await.state;
                    if s == ConnectionState::Closing {
                        let _ = transport_event_tx
                            .send(TransportEvent::Disconnected(Some(Error::new(
                                ErrorCode::ConnectionClosed,
                                "Close timed out",
                            ))))
                            .await;
                    }
                });
            }
            ConnectionState::Connecting => {
                // Abort the in-progress connection.
                if let Some(transport) = self.transport.take() {
                    transport.abort();
                }
                self.set_state(ConnectionState::Closed, None, None).await;
                let err = Error::new(ErrorCode::ConnectionClosed, "Connection closed");
                self.message_queue.fail_all(&err);
                self.fail_queued_messages(&err);
            }
            ConnectionState::Initialized
            | ConnectionState::Disconnected
            | ConnectionState::Suspended => {
                self.set_state(ConnectionState::Closed, None, None).await;
                let err = Error::new(ErrorCode::ConnectionClosed, "Connection closed");
                self.fail_queued_messages(&err);
            }
            ConnectionState::Closed | ConnectionState::Failed => {
                // Already terminal — nothing to do.
            }
            ConnectionState::Closing => {
                // Already closing — nothing to do.
            }
        }
    }

    async fn handle_send(
        &mut self,
        mut msg: ProtocolMessage,
        cb: Option<oneshot::Sender<Result<()>>>,
    ) {
        if self.state.send_messages() {
            // Connected — send immediately.
            let ack_required = msg.ack_required();
            if ack_required {
                msg.msg_serial = Some(self.msg_serial);
                self.msg_serial += 1;
            }

            if let Some(transport) = &self.transport {
                if let Err(e) = transport.send(msg).await {
                    if let Some(cb) = cb {
                        let _ = cb.send(Err(e));
                    }
                    return;
                }
            }

            if ack_required {
                if let Some(cb) = cb {
                    self.message_queue.push(PendingMessage {
                        protocol_message: ProtocolMessage::default(),
                        callback: cb,
                        send_attempted: true,
                    });
                }
            } else if let Some(cb) = cb {
                let _ = cb.send(Ok(()));
            }
        } else if self.state.queue_messages() {
            // Queue for later.
            if let Some(cb) = cb {
                self.queued_messages.push_back(PendingMessage {
                    protocol_message: msg,
                    callback: cb,
                    send_attempted: false,
                });
            } else {
                // Fire-and-forget messages with no callback — queue with a
                // dummy callback that we'll ignore.
                let (tx, _rx) = oneshot::channel();
                self.queued_messages.push_back(PendingMessage {
                    protocol_message: msg,
                    callback: tx,
                    send_attempted: false,
                });
            }
        } else {
            // Can't send or queue (suspended, closing, closed, failed).
            if let Some(cb) = cb {
                let _ = cb.send(Err(Error::new(
                    ErrorCode::ChannelOperationFailed,
                    format!("Cannot send in {} state", self.state),
                )));
            }
        }
    }

    async fn handle_ping(&mut self, tx: oneshot::Sender<Result<Duration>>) {
        if self.state != ConnectionState::Connected {
            let _ = tx.send(Err(Error::new(
                ErrorCode::Disconnected,
                "Not connected",
            )));
            return;
        }

        let id = format!("ping-{}", rand::random::<u32>());
        let mut heartbeat = ProtocolMessage::new(Action::Heartbeat);
        heartbeat.id = Some(id.clone());

        if let Some(transport) = &self.transport {
            if transport.send(heartbeat).await.is_ok() {
                self.pending_pings.push((id, Instant::now(), tx));
                return;
            }
        }

        let _ = tx.send(Err(Error::new(
            ErrorCode::Disconnected,
            "Failed to send heartbeat",
        )));
    }

    // -----------------------------------------------------------------------
    // Protocol message handler
    // -----------------------------------------------------------------------

    async fn handle_protocol_message(&mut self, pm: ProtocolMessage) {
        // Reset idle timer on any inbound message.
        self.last_activity = Instant::now();

        let action = match pm.action() {
            Some(a) => a,
            None => return, // Unknown action — ignore.
        };

        match action {
            Action::Connected => self.on_connected(pm).await,
            Action::Disconnected => self.on_disconnected(pm).await,
            Action::Closed => self.on_closed(pm).await,
            Action::Error => {
                if pm.channel.is_some() {
                    // Channel-level error — route to channel.
                    let _ = self.channel_msg_tx.send(pm);
                } else {
                    // Connection-level error.
                    self.on_connection_error(pm).await;
                }
            }
            Action::Heartbeat => {
                // Resolve any pending ping with matching id.
                if let Some(id) = &pm.id {
                    if let Some(pos) = self
                        .pending_pings
                        .iter()
                        .position(|(pid, _, _)| pid == id)
                    {
                        let (_, sent_at, tx) = self.pending_pings.remove(pos);
                        let _ = tx.send(Ok(sent_at.elapsed()));
                    }
                }
            }
            Action::Ack => {
                if let (Some(serial), Some(count)) = (pm.msg_serial, pm.count) {
                    self.message_queue.complete(serial, count, None);
                }
            }
            Action::Nack => {
                if let (Some(serial), Some(count)) = (pm.msg_serial, pm.count) {
                    let error = pm
                        .error
                        .map(|e| e.into_error())
                        .unwrap_or_else(|| Error::new(ErrorCode::InternalError, "NACK received"));
                    self.message_queue.complete(serial, count, Some(error));
                }
            }
            // Channel-bound messages — route to channel manager.
            Action::Attached
            | Action::Detached
            | Action::Message
            | Action::Presence
            | Action::Sync => {
                let _ = self.channel_msg_tx.send(pm);
            }
            // We don't handle AUTH (server-initiated re-auth) since we're
            // API-key-only. Ignore.
            Action::Auth => {}
            // Internal retry signal (sent by schedule_retry timer).
            Action::Connect => {
                self.handle_internal_connect_signal().await;
            }
            // Client-initiated actions — should not be received from server.
            Action::Disconnect | Action::Close | Action::Attach | Action::Detach => {}
        }
    }

    // -----------------------------------------------------------------------
    // Specific protocol message handlers
    // -----------------------------------------------------------------------

    async fn on_connected(&mut self, pm: ProtocolMessage) {
        if let Some(details) = &pm.connection_details {
            // connection_id may be at the top-level or inside connectionDetails;
            // prefer top-level (the spec sends it there), fall back to details.
            self.connection_id = pm.connection_id.clone()
                .or_else(|| details.connection_id.clone());
            self.connection_key = details.connection_key.clone();

            if let Some(ttl) = details.connection_state_ttl {
                self.connection_state_ttl = Duration::from_millis(ttl);
            }

            // CD2h: Set up idle timer from maxIdleInterval.
            // Add a grace period (realtimeRequestTimeout, default 10s) like ably-js.
            if let Some(max_idle_ms) = details.max_idle_interval {
                let grace = Duration::from_secs(10);
                self.max_idle_interval = Some(Duration::from_millis(max_idle_ms) + grace);
            } else {
                self.max_idle_interval = None;
            }

            // Update client_id if server provided one (RSA7b3).
            if details.client_id.is_some() {
                self.shared.write().await.client_id = details.client_id.clone();
            }

            self.connection_details = Some(details.clone());
        } else {
            // No connectionDetails but top-level connectionId might still be set.
            if pm.connection_id.is_some() {
                self.connection_id = pm.connection_id.clone();
            }
        }

        // Check if this is a resumed connection.
        let resumed = pm.has_flag(flags::RESUMED);

        if !resumed {
            // Not resumed — reset msg_serial.
            self.msg_serial = 0;
        }

        self.retry_count = 0;
        self.suspend_start = None;

        self.set_state(ConnectionState::Connected, None, None).await;

        // Flush queued messages.
        self.send_queued_messages().await;
    }

    async fn on_disconnected(&mut self, pm: ProtocolMessage) {
        let error = pm.error.map(|e| e.into_error());

        // If closing, treat as closed.
        if self.state == ConnectionState::Closing {
            self.on_closed_impl(error).await;
            return;
        }

        // Requeue in-flight messages.
        self.message_queue.drain_to(&mut self.queued_messages);

        // Drop the transport.
        if let Some(transport) = self.transport.take() {
            transport.abort();
        }

        // Check if we should go to suspended.
        if self.should_suspend() {
            self.enter_suspended(error).await;
        } else {
            self.enter_disconnected(error).await;
        }
    }

    async fn on_closed(&mut self, pm: ProtocolMessage) {
        let error = pm.error.map(|e| e.into_error());
        self.on_closed_impl(error).await;
    }

    async fn on_closed_impl(&mut self, error: Option<Error>) {
        if let Some(transport) = self.transport.take() {
            transport.close();
        }

        self.set_state(ConnectionState::Closed, error, None).await;

        let err = Error::new(ErrorCode::ConnectionClosed, "Connection closed");
        self.message_queue.fail_all(&err);
        self.fail_queued_messages(&err);
    }

    async fn on_connection_error(&mut self, mut pm: ProtocolMessage) {
        let error_info = pm.error.take();
        let error = error_info.map(|e| e.into_error());

        if let Some(ref err) = error {
            if !is_retriable(err) {
                // Fatal error — go to failed.
                if let Some(transport) = self.transport.take() {
                    transport.abort();
                }

                let fail_err = Error::new(ErrorCode::ConnectionFailed, "Connection failed");
                self.set_state(ConnectionState::Failed, error, None).await;
                self.message_queue.fail_all(&fail_err);
                self.fail_queued_messages(&fail_err);
                return;
            }
        }

        // Retriable error — reconstruct error in pm for on_disconnected.
        if let Some(err) = &error {
            pm.error = Some(crate::protocol::ErrorInfo {
                code: err.code.code(),
                status_code: err.status_code.unwrap_or(0),
                message: err.message.clone(),
                href: None,
            });
        }
        self.on_disconnected(pm).await;
    }

    // -----------------------------------------------------------------------
    // Transport disconnect handler
    // -----------------------------------------------------------------------

    async fn handle_idle_timeout(&mut self) {
        if self.state != ConnectionState::Connected {
            return;
        }

        let since_last = self.last_activity.elapsed();
        let msg = format!(
            "No activity seen from realtime in {}ms; assuming connection has dropped",
            since_last.as_millis()
        );
        let error = Error::new(ErrorCode::Disconnected, &msg);

        // Tear down the transport and transition to disconnected.
        if let Some(transport) = self.transport.take() {
            transport.abort();
        }

        // Requeue in-flight messages.
        self.message_queue.drain_to(&mut self.queued_messages);

        if self.should_suspend() {
            self.enter_suspended(Some(error)).await;
        } else {
            self.enter_disconnected(Some(error)).await;
        }
    }

    async fn handle_transport_disconnected(&mut self, error: Option<Error>) {
        // Disable idle timer — will be re-set on next CONNECTED.
        self.max_idle_interval = None;

        if self.state == ConnectionState::Closing {
            self.on_closed_impl(error).await;
            return;
        }

        if self.state == ConnectionState::Closed || self.state == ConnectionState::Failed {
            return;
        }

        // Requeue in-flight messages.
        self.message_queue.drain_to(&mut self.queued_messages);

        // Drop the transport.
        self.transport = None;

        if self.should_suspend() {
            self.enter_suspended(error).await;
        } else {
            self.enter_disconnected(error).await;
        }
    }

    // -----------------------------------------------------------------------
    // Connect / reconnect logic
    // -----------------------------------------------------------------------

    async fn start_connect(&mut self) {
        if self.suspend_start.is_none() {
            self.suspend_start = Some(Instant::now());
        }

        // Check connection state freshness before attempting resume.
        // If last activity is too old, discard connection state.
        self.check_connection_state_freshness();

        self.set_state(ConnectionState::Connecting, None, None).await;

        let resume_key = self.connection_key.clone();
        let opts = self.opts.clone();
        let event_tx = self.transport_event_tx.clone();

        match Transport::connect(&opts, resume_key.as_deref(), event_tx).await {
            Ok(transport) => {
                self.transport = Some(transport);
                // Now we wait for a CONNECTED protocol message.
                // Set a connection timeout.
                let timeout_tx = self.transport_event_tx.clone();
                let timeout_duration = Duration::from_secs(20); // connectingTimeout
                tokio::spawn(async move {
                    tokio::time::sleep(timeout_duration).await;
                    let _ = timeout_tx
                        .send(TransportEvent::Disconnected(Some(Error::new(
                            ErrorCode::ConnectionTimedOut,
                            "Connection timed out",
                        ))))
                        .await;
                });
            }
            Err(err) => {
                // Connection attempt failed — go to disconnected/suspended.
                if self.should_suspend() {
                    self.enter_suspended(Some(err)).await;
                } else {
                    self.enter_disconnected(Some(err)).await;
                }
            }
        }
    }

    async fn enter_disconnected(&mut self, error: Option<Error>) {
        let delay = self.retry_delay(self.opts.disconnected_retry_timeout);
        self.retry_count += 1;

        self.set_state(ConnectionState::Disconnected, error, Some(delay))
            .await;

        // Schedule retry.
        self.schedule_retry(delay).await;
    }

    async fn enter_suspended(&mut self, error: Option<Error>) {
        let delay = self.opts.suspended_retry_timeout;

        // Fail queued messages — suspended state doesn't queue.
        let err = Error::new(ErrorCode::ConnectionSuspended, "Connection suspended");
        self.fail_queued_messages(&err);

        self.set_state(ConnectionState::Suspended, error, Some(delay))
            .await;

        // Schedule retry.
        self.schedule_retry(delay).await;
    }

    async fn schedule_retry(&self, delay: Duration) {
        let event_tx = self.transport_event_tx.clone();
        let shared = self.shared.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            // Only retry if we're still in disconnected/suspended.
            let s = shared.read().await.state;
            if s == ConnectionState::Disconnected || s == ConnectionState::Suspended {
                // Send a synthetic event to trigger reconnect. We use a
                // protocol message with a Connect action as an internal signal.
                let signal = ProtocolMessage::new(Action::Connect);
                let _ = event_tx.send(TransportEvent::Message(signal)).await;
            }
        });
    }

    /// Check if we've been trying to connect long enough to enter suspended.
    /// Discard connection state if it's too stale for resume to work.
    fn check_connection_state_freshness(&mut self) {
        // Only check if we have a connection to resume.
        if self.connection_key.is_none() {
            return;
        }

        let since_last = self.last_activity.elapsed();
        let max_stale = self.connection_state_ttl
            + self.max_idle_interval.unwrap_or(Duration::ZERO);

        if since_last > max_stale {
            // Connection state is too old — discard it so we don't
            // attempt a resume that will fail.
            self.connection_id = None;
            self.connection_key = None;
            self.connection_details = None;
            self.msg_serial = 0;
        }
    }

    fn should_suspend(&self) -> bool {
        if let Some(start) = self.suspend_start {
            start.elapsed() >= self.connection_state_ttl
        } else {
            false
        }
    }

    /// Calculate retry delay with jitter.
    fn retry_delay(&self, base: Duration) -> Duration {
        let count = self.retry_count.min(7) as f64;
        let jitter = 1.0 + rand::random::<f64>() * count * 0.1;
        Duration::from_secs_f64(base.as_secs_f64() * jitter)
    }

    // -----------------------------------------------------------------------
    // Message sending
    // -----------------------------------------------------------------------

    async fn send_queued_messages(&mut self) {
        while let Some(mut pending) = self.queued_messages.pop_front() {
            let msg = &mut pending.protocol_message;
            let ack_required = msg.ack_required();

            if ack_required && !pending.send_attempted {
                msg.msg_serial = Some(self.msg_serial);
                self.msg_serial += 1;
            }

            if let Some(transport) = &self.transport {
                if let Err(e) = transport.send(pending.protocol_message.clone()).await {
                    let _ = pending.callback.send(Err(e));
                    continue;
                }
            }

            if ack_required {
                self.message_queue.push(pending);
            } else {
                let _ = pending.callback.send(Ok(()));
            }
        }
    }

    fn fail_queued_messages(&mut self, error: &Error) {
        while let Some(pending) = self.queued_messages.pop_front() {
            let _ = pending.callback.send(Err(Error::new(
                error.code,
                error.message.clone(),
            )));
        }
    }
}

// Handle the internal Connect signal from the retry timer.
impl ConnectionManager {
    async fn handle_internal_connect_signal(&mut self) {
        if self.state == ConnectionState::Disconnected
            || self.state == ConnectionState::Suspended
        {
            self.start_connect().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Whether an error is retriable (connection should attempt reconnect).
fn is_retriable(err: &Error) -> bool {
    match err.status_code {
        Some(code) if code >= 500 => true,
        Some(_) => {
            // Check if it's a known connection error code.
            matches!(
                err.code,
                ErrorCode::Disconnected
                    | ErrorCode::ConnectionSuspended
                    | ErrorCode::ConnectionFailed
                    | ErrorCode::ConnectionTimedOut
                    | ErrorCode::InternalConnectionError
            )
        }
        None => true,
    }
}
