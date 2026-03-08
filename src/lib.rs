//! A fully-featured real-time Rust client for [Ably].
//!
//! # Example
//!
//! ```rust,no_run
//! use reliably::{AblyClient, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = AblyClient::new("<api_key>")?;
//!     let channel = client.channels().get("my-channel");
//!     channel.publish().string("hello").send().await?;
//!     Ok(())
//! }
//! ```
//!
//! [Ably]: https://ably.com

#[macro_use]
pub mod error;
pub mod auth;
pub mod crypto;
pub mod http;
mod json;
pub mod options;
pub mod presence;
pub mod connection;
pub mod protocol;
pub mod realtime;
pub mod realtime_channel;
pub mod realtime_presence;
pub mod rest;
pub(crate) mod transport;
pub mod stats;

pub use error::{Error, Result};
pub use options::ClientOptions;
pub use rest::{Data, Rest};
pub use realtime::Realtime;
pub use connection::{Connection, ConnectionState, ConnectionStateChange};
pub use realtime_channel::{Channels, RealtimeChannel, ChannelState, ChannelStateChange};

// Ably-prefixed type aliases for convenience.
/// Alias for [`Rest`] — the main Ably REST client.
pub type AblyClient = Rest;
/// Alias for [`ClientOptions`] — configuration for the Ably client.
pub type AblyClientOptions = ClientOptions;

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::iter::FromIterator;
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use futures::TryStreamExt;
    use reqwest::Url;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::auth::{AuthOptions, Credential, TokenParams};
    use crate::connection::ConnectionState;
    use crate::error::ErrorCode;
    use crate::http::Method;
    use crate::realtime::Realtime;
    use crate::realtime_channel::ChannelState;

    #[test]
    fn rest_client_from_string_with_colon_sets_key() {
        let s = "appID.keyID:keySecret";
        let client = Rest::new(s).unwrap();
        assert!(matches!(client.inner.opts.credential, Credential::Key(_)));
    }

    #[test]
    fn rest_client_from_string_without_colon_sets_token_literal() {
        let s = "appID.tokenID";
        let client = Rest::new(s).unwrap();
        assert!(matches!(
            client.inner.opts.credential,
            Credential::TokenDetails(_)
        ));
    }

    fn test_client() -> Rest {
        ClientOptions::new("aaaaaa.bbbbbb:cccccc")
            .environment("sandbox")
            .unwrap()
            .rest()
            .unwrap()
    }

    /// A test app in the Ably Sandbox environment.
    #[derive(Clone, Debug, Deserialize)]
    struct TestApp {
        keys: Vec<auth::Key>,
    }

    impl auth::AuthCallback for TestApp {
        fn token<'a>(
            &'a self,
            params: &'a TokenParams,
        ) -> std::pin::Pin<
            Box<dyn Send + futures::Future<Output = Result<auth::RequestOrDetails>> + 'a>,
        > {
            let fut = async { Ok(auth::RequestOrDetails::Request(self.token_request(params)?)) };
            Box::pin(fut)
        }
    }

    impl TestApp {
        /// Creates a test app in the Ably Sandbox environment with a single
        /// API key.
        async fn create() -> Result<Self> {
            let spec = json!({
                "keys": [
                    {}
                ],
                "namespaces": [
                    { "id": "persisted", "persisted": true },
                    { "id": "pushenabled", "pushEnabled": true }
                ],
                "channels": [
                    {
                        "name": "persisted:presence_fixtures",
                        "presence": [
                            {
                                "clientId": "client_string",
                                "data": "some presence data"
                            },
                            {
                                "clientId": "client_json",
                                "data": "{\"some\":\"presence data\"}",
                                "encoding": "json"
                            },
                            {
                                "clientId": "client_binary",
                                "data": "c29tZSBwcmVzZW5jZSBkYXRh",
                                "encoding": "base64"
                            }
                        ]
                    }
                ]
            });

            test_client()
                .request(Method::POST, "/apps")
                .body(&spec)
                .send()
                .await?
                .body()
                .await
        }

        /// Returns a Rest client with the test app's key.
        fn client(&self) -> Rest {
            self.options().rest().unwrap()
        }

        fn options(&self) -> ClientOptions {
            ClientOptions::with_key(self.key())
                .environment("sandbox")
                .unwrap()
        }

        fn key(&self) -> auth::Key {
            self.keys[0].clone()
        }

        fn token_request(&self, params: &auth::TokenParams) -> Result<auth::TokenRequest> {
            self.key().sign(params)
        }

        fn auth_options(&self) -> AuthOptions {
            AuthOptions {
                token: Some(self.options().credential),
                headers: None,
                method: Default::default(),
                params: None,
            }
        }

        /// Returns a Realtime client configured for the sandbox environment.
        fn realtime_client(&self) -> Realtime {
            let opts = self.options().use_binary_protocol(false);
            Realtime::from_options(opts).expect("Expected realtime client to initialise")
        }

        /// Returns a Realtime client with auto_connect disabled.
        fn realtime_client_no_connect(&self) -> Realtime {
            let mut opts = self.options().use_binary_protocol(false);
            opts.auto_connect = false;
            Realtime::from_options(opts).expect("Expected realtime client to initialise")
        }

        /// Returns a Realtime client with a client_id set (required for presence).
        fn realtime_client_with_id(&self, client_id: &str) -> Realtime {
            let opts = self.options()
                .use_binary_protocol(false)
                .client_id(client_id)
                .expect("Expected client_id to be valid");
            Realtime::from_options(opts).expect("Expected realtime client to initialise")
        }
    }

    // TODO: impl Drop for TestApp which deletes the app (needs to be sync)

    #[tokio::test]
    async fn time_returns_the_server_time() -> Result<()> {
        let client = test_client();

        let five_minutes_ago = Utc::now() - Duration::minutes(5);

        let time = client.time().await?;
        assert!(
            time > five_minutes_ago,
            "Expected server time {} to be within the last 5 minutes",
            time
        );

        Ok(())
    }

    #[tokio::test]
    async fn custom_request_returns_body() -> Result<()> {
        let client = test_client();

        let res = client.request(Method::GET, "/time").send().await?;

        let items: Vec<u64> = res.body().await?;

        assert_eq!(items.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn paginated_request_returns_items() -> Result<()> {
        let client = test_client();

        let res = client
            .paginated_request::<json::Value>(Method::GET, "/time")
            .send()
            .await?;

        let items = res.items().await?;

        assert_eq!(items.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn paginated_request_returns_pages() -> Result<()> {
        let client = test_client();

        let mut pages = client
            .paginated_request::<json::Value>(Method::GET, "/time")
            .pages()
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(pages.len(), 1);

        let page = pages.pop().expect("Expected a page");

        let items = page.items().await?;

        assert_eq!(items.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn custom_request_with_unknown_path_returns_404_response() -> Result<()> {
        let client = test_client();

        let err = client
            .request(Method::GET, "/invalid")
            .send()
            .await
            .expect_err("Expected 404 error");

        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(err.status_code, Some(404));

        Ok(())
    }

    #[tokio::test]
    async fn custom_request_with_bad_rest_host_returns_network_error() -> Result<()> {
        let client = ClientOptions::new("aaaaaa.bbbbbb:cccccc")
            .rest_host("i-dont-exist.ably.com")?
            .rest()?;

        let err = client
            .request(Method::GET, "/time")
            .send()
            .await
            .expect_err("Expected network error");

        assert_eq!(err.code, ErrorCode::BadRequest);

        Ok(())
    }

    #[tokio::test]
    async fn stats_minute_forwards() -> Result<()> {
        // Create a test app and client.
        let app = TestApp::create().await?;
        let client = app.client();

        let year = 2010;
        let fixtures = json!([
            {
                "intervalId": format!("{}-02-03:15:03", year),
                "inbound": { "realtime": { "messages": { "count": 50, "data": 5000 } } },
                "outbound": { "realtime": { "messages": { "count": 20, "data": 2000 } } }
            },
            {
                "intervalId": format!("{}-02-03:15:04", year),
                "inbound": { "realtime": { "messages": { "count": 60, "data": 6000 } } },
                "outbound": { "realtime": { "messages": { "count": 10, "data": 1000 } } }
            },
            {
                "intervalId": format!("{}-02-03:15:05", year),
                "inbound": { "realtime": { "messages": { "count": 70, "data": 7000 } } },
                "outbound": { "realtime": { "messages": { "count": 40, "data": 4000 } } }
            }
        ]);

        client
            .request(Method::POST, "/stats")
            .body(&fixtures)
            .send()
            .await?;

        // Retrieve the stats.
        let res = client
            .stats()
            .start(format!("{}-02-03:15:03", year).as_ref())
            .end(format!("{}-02-03:15:05", year).as_ref())
            .forwards()
            .send()
            .await?;

        // Check the stats are what we expect.
        let stats = res.items().await?;
        assert_eq!(stats.len(), 3);
        assert_eq!(
            stats
                .iter()
                .map(|s| s.inbound.as_ref().unwrap().all.messages.count)
                .sum::<f64>(),
            50.0 + 60.0 + 70.0
        );
        assert_eq!(
            stats
                .iter()
                .map(|s| s.outbound.as_ref().unwrap().all.messages.count)
                .sum::<f64>(),
            20.0 + 10.0 + 40.0
        );

        Ok(())
    }

    #[test]
    fn auth_create_token_request() -> Result<()> {
        let client = test_client();

        let params = TokenParams {
            capability: r#"{"*":["*"]}"#.to_string(),
            client_id: Some("test@ably.com".to_string()),
            nonce: None,
            timestamp: None,
            ttl: Duration::minutes(100),
        };

        let options = AuthOptions {
            token: Some(client.options().credential.clone()),
            ..Default::default()
        };

        let req = client.auth().create_token_request(&params, &options)?;

        assert_eq!(req.capability, params.capability);
        assert_eq!(req.client_id, params.client_id);
        assert_eq!(req.ttl, params.ttl);

        Ok(())
    }

    #[tokio::test]
    async fn auth_request_token_with_key() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Get the server time.
        let server_time = client.time().await?;

        // Request a token.
        let token = client
            .auth()
            .request_token(&Default::default(), &app.auth_options())
            .await?;
        let meta = token.metadata.unwrap();

        // Check the token details.
        assert!(!token.token.is_empty(), "Expected token to be set");
        assert!(
            meta.issued >= server_time,
            "Expected issued ({}) to be after server time ({})",
            meta.issued,
            server_time,
        );
        assert!(
            meta.expires > meta.issued,
            "Expected expires ({}) to be after issued ({})",
            meta.expires,
            meta.issued
        );
        let capability = meta.capability;
        assert_eq!(
            capability, r#"{"*":["*"]}"#,
            r#"Expected default capability '{{"*":["*"]}}', got {}"#,
            capability
        );
        assert_eq!(
            meta.client_id,
            None,
            "Expected client_id to be null, got {}",
            meta.client_id.as_ref().unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn auth_request_token_with_auth_url() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Generate an authUrl.
        let key = app.key();
        let auth_url = Url::parse_with_params(
            "https://echo.ably.io/createJWT",
            &[("keyName", key.name), ("keySecret", key.value)],
        )
        .unwrap();

        let options = AuthOptions {
            token: Some(Credential::Url(auth_url)),
            ..AuthOptions::default()
        };

        let token = client
            .auth()
            .request_token(&Default::default(), &options)
            .await?;

        // Check the token details.
        assert!(!token.token.is_empty(), "Expected token to be set");

        Ok(())
    }

    #[tokio::test]
    async fn auth_request_token_with_provider() -> Result<()> {
        // Create a test app.
        let app = Arc::new(TestApp::create().await?);
        let client = app.client();

        let token = client
            .auth()
            .request_token(&Default::default(), &app.auth_options())
            .await?;

        // Check the token details.
        assert!(!token.token.is_empty(), "Expected token to be set");

        Ok(())
    }

    #[tokio::test]
    async fn auth_request_token_with_client_id_in_options() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;

        // Create a client with client_id set in the options.
        let client_id = "test client id";
        let client = app.options().client_id(client_id)?.rest()?;
        let options = TokenParams {
            client_id: Some(client_id.to_string()),
            ..Default::default()
        };

        // Request a token.
        let token = client
            .auth()
            .request_token(&options, &app.auth_options())
            .await?;

        // Check the token details include the client_id.
        assert!(!token.token.is_empty(), "Expected token to be set");
        assert_eq!(
            token.metadata.unwrap().client_id,
            Some(client_id.to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn channel_publish_string() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish a message with string data.
        let channel = client.channels().get("test_channel_publish_string");
        let data = "a string";
        channel.publish().name("name").string(data).send().await?;

        // Retrieve the message from history.
        let res = channel.history().send().await?;
        let mut history = res.items().await?;
        let message = history.pop().expect("Expected a history message");
        assert_eq!(message.data, Data::String(data.to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn channel_publish_json_object() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish a message with JSON serializable data.
        let channel = client.channels().get("test_channel_publish_json_object");
        #[derive(Serialize)]
        struct TestData<'a> {
            b: bool,
            i: i64,
            s: &'a str,
            o: HashMap<&'a str, &'a str>,
            v: Vec<i64>,
        }
        let data = TestData {
            b: true,
            i: 42,
            s: "a string",
            o: [("x", "1"), ("y", "2")].iter().cloned().collect(),
            v: vec![1, 2, 3],
        };
        channel.publish().name("name").json(data).send().await?;

        // Retrieve the message from history.
        let res = channel.history().send().await?;
        let mut history = res.items().await?;
        let message = history.pop().expect("Expected a history message");
        let json = serde_json::json!({
            "b": true,
            "i": 42,
            "s": "a string",
            "o": {"x": "1", "y": "2"},
            "v": [1, 2, 3]
        });
        assert_eq!(message.data, Data::JSON(json));

        Ok(())
    }

    #[tokio::test]
    async fn channel_publish_binary() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish a message with binary data.
        let channel = client.channels().get("test_channel_publish_binary");
        let data = vec![0x1, 0x2, 0x3, 0x4];
        channel.publish().name("name").binary(data).send().await?;

        // Retrieve the message from history.
        let res = channel.history().send().await?;
        let mut history = res.items().await?;
        let message = history.pop().expect("Expected a history message");
        assert_eq!(message.data, vec![0x1, 0x2, 0x3, 0x4].into());

        Ok(())
    }

    #[tokio::test]
    async fn channel_publish_extras() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish a message with extras.
        let channel = client.channels().get("test_channel_publish_extras");
        let data = "a string";
        let mut extras = json::Map::new();
        extras.insert("headers".to_string(), json!({"some":"metadata"}));
        channel
            .publish()
            .name("name")
            .string(data)
            .extras(extras.clone())
            .send()
            .await?;

        // Retrieve the message from history.
        let res = channel.history().send().await?;
        let mut history = res.items().await?;
        let message = history.pop().expect("Expected a history message");
        assert_eq!(message.extras, Some(extras));

        Ok(())
    }

    #[tokio::test]
    async fn channel_publish_params() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish a message with params '_forceNack=true' which should
        // result in the publish being rejected with a 40099 error code
        let channel = client.channels().get("test_channel_publish_params");
        let data = "a string";
        let err = channel
            .publish()
            .name("name")
            .string(data)
            .params(&[("_forceNack", "true")])
            .send()
            .await
            .expect_err("Expected realtime to reject the publish with _forceNack=true");
        assert_eq!(err.code, ErrorCode::Testing);

        Ok(())
    }

    #[tokio::test]
    async fn channel_presence_get() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Retrieve the presence set
        let channel = client.channels().get("persisted:presence_fixtures");
        let res = channel.presence.get().send().await?;
        let presence = res.items().await?;
        assert_eq!(presence.len(), 3);
        assert_eq!(presence[0].data, "some presence data".as_bytes().into());
        assert_eq!(
            presence[1].data,
            Data::JSON(serde_json::json!({"some":"presence data"}))
        );
        assert_eq!(
            presence[2].data,
            Data::String("some presence data".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn channel_presence_history() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Retrieve the presence history
        let channel = client.channels().get("persisted:presence_fixtures");
        let res = channel.presence.history().send().await?;
        let presence = res.items().await?;
        assert_eq!(presence.len(), 3);
        assert_eq!(presence[0].data, "some presence data".as_bytes().into());
        assert_eq!(
            presence[1].data,
            Data::JSON(serde_json::json!({"some":"presence data"}))
        );
        assert_eq!(
            presence[2].data,
            Data::String("some presence data".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn channel_history_count() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish some messages.
        let channel = client.channels().get("persisted:history_count");
        futures::try_join!(
            channel.publish().name("event0").string("some data").send(),
            channel
                .publish()
                .name("event1")
                .string("some more data")
                .send(),
            channel.publish().name("event2").string("and more").send(),
            channel.publish().name("event3").string("and more").send(),
            channel.publish().name("event4").json(vec![1, 2, 3]).send(),
            channel
                .publish()
                .name("event5")
                .json(json!({"one": 1, "two": 2, "three": 3}))
                .send(),
            channel
                .publish()
                .name("event6")
                .json(json!({"foo": "bar"}))
                .send(),
        )?;

        // Wait a second.
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Retrieve the channel history.
        let mut pages = channel.history().pages().try_collect::<Vec<_>>().await?;
        assert_eq!(pages.len(), 1);
        let history = pages.pop().unwrap().items().await?;
        assert_eq!(history.len(), 7, "Expected 7 history messages");

        // Check message IDs are unique.
        let ids = HashSet::<_>::from_iter(history.iter().map(|msg| msg.id.as_ref().unwrap()));
        assert_eq!(ids.len(), 7, "Expected 7 unique ids");

        Ok(())
    }

    #[tokio::test]
    async fn channel_history_paginate_backwards() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;
        let client = app.client();

        // Publish some messages.
        let channel = client
            .channels()
            .get("persisted:history_paginate_backwards");
        channel
            .publish()
            .name("event0")
            .string("some data")
            .send()
            .await?;
        channel
            .publish()
            .name("event1")
            .string("some more data")
            .send()
            .await?;
        channel
            .publish()
            .name("event2")
            .string("and more")
            .send()
            .await?;
        channel
            .publish()
            .name("event3")
            .string("and more")
            .send()
            .await?;
        channel
            .publish()
            .name("event4")
            .json(vec![1, 2, 3])
            .send()
            .await?;
        channel
            .publish()
            .name("event5")
            .json(json!({"one": 1, "two": 2, "three": 3}))
            .send()
            .await?;
        channel
            .publish()
            .name("event6")
            .json(json!({"foo": "bar"}))
            .send()
            .await?;

        // Wait a second.
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // Retrieve the channel history backwards one message at a time.
        let mut pages = channel.history().backwards().limit(1).pages();

        // Check each page has the expected items.
        for (expected_name, expected_data) in [
            ("event6", Data::JSON(json!({"foo": "bar"}))),
            ("event5", Data::JSON(json!({"one":1,"two":2,"three":3}))),
            ("event4", Data::JSON(json!([1, 2, 3]))),
            ("event3", Data::String("and more".to_string())),
            ("event2", Data::String("and more".to_string())),
            ("event1", Data::String("some more data".to_string())),
            ("event0", Data::String("some data".to_string())),
        ] {
            let page = pages.try_next().await?.expect("Expected a page");
            let mut history = page.items().await?;
            assert_eq!(history.len(), 1, "Expected 1 history message per page");
            let message = history.pop().unwrap();
            assert_eq!(message.name, Some(expected_name.to_string()));
            assert_eq!(message.data, expected_data);
        }

        Ok(())
    }

    #[tokio::test]
    async fn client_fallback() -> Result<()> {
        // IANA reserved; requests to it will hang forever
        let unroutable_host = "10.255.255.1";
        let client = ClientOptions::new("aaaaaa.bbbbbb:cccccc")
            .rest_host(unroutable_host)?
            .fallback_hosts(vec!["sandbox-a-fallback.ably-realtime.com".to_string()])
            .http_request_timeout(std::time::Duration::from_secs(3))
            .rest()?;

        client.time().await.expect("Expected fallback response");

        Ok(())
    }

    #[tokio::test]
    async fn rest_with_auth_url() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;

        // Generate an authUrl.
        let key = app.key();
        let auth_url = Url::parse_with_params(
            "https://echo.ably.io/createJWT",
            &[("keyName", key.name), ("keySecret", key.value)],
        )
        .unwrap();

        // Configure a client with an authUrl.
        let client = ClientOptions::with_auth_url(auth_url)
            .environment("sandbox")?
            .rest()
            .expect("Expected client to initialise");

        // Check a REST request succeeds.
        client
            .stats()
            .send()
            .await
            .expect("Expected REST request to succeed");

        Ok(())
    }

    #[tokio::test]
    async fn rest_with_auth_callback() -> Result<()> {
        // Create a test app.
        let app = Arc::new(TestApp::create().await?);

        // Configure a client with the test app as the authCallback.
        let client = ClientOptions::with_auth_callback(app)
            .environment("sandbox")?
            .rest()
            .expect("Expected client to initialise");

        // Check a REST request succeeds.
        client
            .stats()
            .send()
            .await
            .expect("Expected REST request to succeed");

        Ok(())
    }

    #[tokio::test]
    async fn rest_with_key_and_use_token_auth() -> Result<()> {
        // Create a test app.
        let app = TestApp::create().await?;

        // Configure a client with a key and useTokenAuth=true.
        let client = ClientOptions::with_key(app.key())
            .use_token_auth(true)
            .environment("sandbox")?
            .rest()
            .expect("Expected client to initialise");

        // Check a REST request succeeds.
        client
            .stats()
            .send()
            .await
            .expect("Expected REST request to succeed");

        Ok(())
    }

    // ===================================================================
    // Realtime tests
    // ===================================================================

    /// Timeout for connection/channel state waits in tests.
    const STATE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    /// Timeout for receiving messages in tests.
    const MSG_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    #[tokio::test]
    async fn realtime_connect() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        // Race-free: returns immediately if already connected.
        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        assert_eq!(client.connection.state(), ConnectionState::Connected);

        let id = client.connection.id().await;
        assert!(id.is_some(), "Expected connection ID to be set");

        let key = client.connection.key().await;
        assert!(key.is_some(), "Expected connection key to be set");

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_connect_manual() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client_no_connect();

        assert_eq!(client.connection.state(), ConnectionState::Initialized);

        client.connection.connect();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;
        assert_eq!(client.connection.state(), ConnectionState::Connected);

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_close() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        client.close().await;

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Closed, STATE_TIMEOUT)
            .await?;
        assert_eq!(client.connection.state(), ConnectionState::Closed);

        Ok(())
    }

    #[tokio::test]
    async fn realtime_ping() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let rtt = client.connection.ping().await?;
        assert!(
            rtt.as_millis() < 10_000,
            "Expected ping RTT under 10s, got {:?}",
            rtt
        );

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_channel_attach() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_attach").await;
        assert_eq!(channel.state(), ChannelState::Initialized);

        channel.attach().await?;
        assert_eq!(channel.state(), ChannelState::Attached);

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_channel_detach() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_detach").await;
        channel.attach().await?;
        assert_eq!(channel.state(), ChannelState::Attached);

        channel.detach().await?;
        assert_eq!(channel.state(), ChannelState::Detached);

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_publish_subscribe() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_pubsub").await;
        let mut sub = channel.subscribe().await?;

        channel
            .publish(Some("greeting"), Data::String("hello world".into()))
            .await?;

        let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
            .await
            .expect("Timed out waiting for message")
            .expect("Subscription channel closed");

        assert_eq!(msg.name, Some("greeting".to_string()));
        assert_eq!(msg.data, Data::String("hello world".to_string()));

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_publish_subscribe_json() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_pubsub_json").await;
        let mut sub = channel.subscribe().await?;

        let data = json!({"key": "value", "num": 42});
        channel
            .publish(Some("json_event"), Data::JSON(data.clone()))
            .await?;

        let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
            .await
            .expect("Timed out waiting for message")
            .expect("Subscription channel closed");

        assert_eq!(msg.name, Some("json_event".to_string()));
        assert_eq!(msg.data, Data::JSON(data));

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_publish_multiple() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_publish_multi").await;
        let mut sub = channel.subscribe().await?;

        for i in 0..3 {
            channel
                .publish(
                    Some(&format!("event_{}", i)),
                    Data::String(format!("data_{}", i)),
                )
                .await?;
        }

        let mut received = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
                .await
                .expect("Timed out waiting for message")
                .expect("Subscription channel closed");
            received.push(msg);
        }

        assert_eq!(received.len(), 3);
        for (i, msg) in received.iter().enumerate() {
            assert_eq!(msg.name, Some(format!("event_{}", i)));
            assert_eq!(msg.data, Data::String(format!("data_{}", i)));
        }

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_channel_state_changes() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_state_changes").await;

        channel.attach().await?;
        assert_eq!(channel.state(), ChannelState::Attached);

        channel.detach().await?;
        assert_eq!(channel.state(), ChannelState::Detached);

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_subscribe_auto_attaches() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_realtime_auto_attach").await;
        assert_eq!(channel.state(), ChannelState::Initialized);

        let _sub = channel.subscribe().await?;
        assert_eq!(channel.state(), ChannelState::Attached);

        client.close().await;
        Ok(())
    }

    // ===================================================================
    // Concurrent subscriber tests
    // ===================================================================

    #[tokio::test]
    async fn realtime_multiple_subscribers_same_channel() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_multi_sub").await;

        // Create 3 independent subscribers.
        let mut sub1 = channel.subscribe().await?;
        let mut sub2 = channel.subscribe().await?;
        let mut sub3 = channel.subscribe().await?;

        // Publish a single message.
        channel
            .publish(Some("event"), Data::String("broadcast".into()))
            .await?;

        // All 3 subscribers should receive the same message.
        for (i, sub) in [&mut sub1, &mut sub2, &mut sub3].iter_mut().enumerate() {
            let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
                .await
                .unwrap_or_else(|_| panic!("Subscriber {} timed out", i + 1))
                .unwrap_or_else(|| panic!("Subscriber {} channel closed", i + 1));

            assert_eq!(msg.name, Some("event".to_string()));
            assert_eq!(msg.data, Data::String("broadcast".to_string()));
        }

        client.close().await;
        Ok(())
    }

    // ===================================================================
    // High-throughput test
    // ===================================================================

    #[tokio::test]
    async fn realtime_high_throughput_ordered_delivery() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_high_throughput").await;
        let mut sub = channel.subscribe().await?;

        let count = 20;

        // Publish messages sequentially (each waits for ACK).
        for i in 0..count {
            channel
                .publish(
                    Some(&format!("msg_{}", i)),
                    Data::String(format!("payload_{}", i)),
                )
                .await?;
        }

        // Receive all messages. Use a generous per-message timeout since
        // messages echo back from the server.
        let mut received = Vec::new();
        for i in 0..count {
            let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
                .await
                .unwrap_or_else(|_| panic!(
                    "Timed out waiting for message {}/{} (received {} so far)",
                    i, count, received.len()
                ))
                .unwrap_or_else(|| panic!(
                    "Subscription closed after receiving {}/{} messages",
                    received.len(), count
                ));
            received.push(msg);
        }

        assert_eq!(received.len(), count);
        for (i, msg) in received.iter().enumerate() {
            assert_eq!(
                msg.name,
                Some(format!("msg_{}", i)),
                "Message {} out of order",
                i
            );
            assert_eq!(msg.data, Data::String(format!("payload_{}", i)));
        }

        client.close().await;
        Ok(())
    }

    // ===================================================================
    // Presence tests
    // ===================================================================

    #[tokio::test]
    async fn realtime_presence_enter_leave() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client_with_id("test-user-1");

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_presence_enter_leave").await;
        channel.attach().await?;

        // Subscribe to presence events before entering.
        let mut presence_sub = channel.presence.subscribe();

        // Enter presence.
        channel
            .presence
            .enter(Some(Data::String("online".into())))
            .await?;

        // Should receive the enter event.
        let event = tokio::time::timeout(MSG_TIMEOUT, presence_sub.recv())
            .await
            .expect("Timed out waiting for presence enter event")
            .expect("Presence subscription closed");

        assert_eq!(event.client_id, "test-user-1");
        assert!(
            matches!(event.action, crate::rest::PresenceAction::Enter),
            "Expected Enter action, got {:?}",
            event.action
        );

        // Verify the member is in the presence set.
        let members = channel.presence.get().await;
        assert!(
            members.iter().any(|m| m.client_id == "test-user-1"),
            "Expected test-user-1 in presence set, got: {:?}",
            members
        );

        // Leave presence.
        channel.presence.leave(None).await?;

        // Should receive the leave event.
        let event = tokio::time::timeout(MSG_TIMEOUT, presence_sub.recv())
            .await
            .expect("Timed out waiting for presence leave event")
            .expect("Presence subscription closed");

        assert_eq!(event.client_id, "test-user-1");
        assert!(
            matches!(event.action, crate::rest::PresenceAction::Leave),
            "Expected Leave action, got {:?}",
            event.action
        );

        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_presence_update() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client_with_id("test-user-2");

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_presence_update").await;
        channel.attach().await?;

        let mut presence_sub = channel.presence.subscribe();

        // Enter first.
        channel
            .presence
            .enter(Some(Data::String("status: idle".into())))
            .await?;

        // Drain any events from enter and presence sync. Give a brief
        // window for the sync protocol to complete.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        while presence_sub.try_recv().is_ok() {}

        // Update presence data.
        channel
            .presence
            .update(Some(Data::String("status: active".into())))
            .await?;

        // Should receive the update event. The server may echo back as
        // Update or Present depending on sync timing.
        let event = tokio::time::timeout(MSG_TIMEOUT, presence_sub.recv())
            .await
            .expect("Timed out waiting for presence update event")
            .expect("Presence subscription closed");

        assert_eq!(event.client_id, "test-user-2");
        assert!(
            matches!(
                event.action,
                crate::rest::PresenceAction::Update | crate::rest::PresenceAction::Present
            ),
            "Expected Update or Present action, got {:?}",
            event.action
        );

        // Leave.
        channel.presence.leave(None).await?;
        client.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn realtime_presence_get_members() -> Result<()> {
        let app = TestApp::create().await?;

        // Two clients join the same channel.
        let client_a = app.realtime_client_with_id("user-a");
        let client_b = app.realtime_client_with_id("user-b");

        client_a
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;
        client_b
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel_a = client_a.channels.get("test_presence_get").await;
        let channel_b = client_b.channels.get("test_presence_get").await;

        channel_a.attach().await?;
        channel_b.attach().await?;

        // Both enter.
        channel_a
            .presence
            .enter(Some(Data::String("a-data".into())))
            .await?;
        channel_b
            .presence
            .enter(Some(Data::String("b-data".into())))
            .await?;

        // Give the server a moment to propagate presence to both channels.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Channel A should see both members.
        let members_a = channel_a.presence.get().await;
        let ids_a: Vec<&str> = members_a.iter().map(|m| m.client_id.as_str()).collect();
        assert!(
            ids_a.contains(&"user-a") && ids_a.contains(&"user-b"),
            "Expected both users in presence set on channel_a, got: {:?}",
            ids_a
        );

        // Channel B should also see both members.
        let members_b = channel_b.presence.get().await;
        let ids_b: Vec<&str> = members_b.iter().map(|m| m.client_id.as_str()).collect();
        assert!(
            ids_b.contains(&"user-a") && ids_b.contains(&"user-b"),
            "Expected both users in presence set on channel_b, got: {:?}",
            ids_b
        );

        // Clean up.
        channel_a.presence.leave(None).await?;
        channel_b.presence.leave(None).await?;
        client_a.close().await;
        client_b.close().await;
        Ok(())
    }

    // ===================================================================
    // Discontinuity test
    // ===================================================================

    #[tokio::test]
    async fn realtime_discontinuity_on_first_attach() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_discontinuity").await;

        // Subscribe to discontinuity events BEFORE attaching.
        let mut disc_rx = channel.on_discontinuity();

        // Attach — first attach is always non-resumed, so a discontinuity
        // event should fire (RTL18: fresh attach from attaching state).
        channel.attach().await?;

        // The discontinuity event should be emitted.
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            disc_rx.recv(),
        )
        .await
        .expect("Timed out waiting for discontinuity event")
        .expect("Discontinuity subscription closed");

        assert_eq!(event.current, ChannelState::Attached);
        assert!(!event.resumed, "First attach should not be resumed");

        client.close().await;
        Ok(())
    }

    // ===================================================================
    // Two-client pub/sub test (messages cross connections)
    // ===================================================================

    #[tokio::test]
    async fn realtime_two_client_pubsub() -> Result<()> {
        let app = TestApp::create().await?;
        let publisher = app.realtime_client();
        let subscriber = app.realtime_client();

        publisher
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;
        subscriber
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let pub_ch = publisher.channels.get("test_two_client").await;
        let sub_ch = subscriber.channels.get("test_two_client").await;

        let mut sub = sub_ch.subscribe().await?;
        // Ensure the subscriber channel is attached before publishing.
        sub_ch
            .wait_for_state(ChannelState::Attached)
            .await?;

        // Publish from the publisher client.
        pub_ch
            .publish(Some("cross"), Data::String("from-another-client".into()))
            .await?;

        // Subscriber should receive it.
        let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
            .await
            .expect("Timed out waiting for cross-client message")
            .expect("Subscription closed");

        assert_eq!(msg.name, Some("cross".to_string()));
        assert_eq!(
            msg.data,
            Data::String("from-another-client".to_string())
        );

        publisher.close().await;
        subscriber.close().await;
        Ok(())
    }

    // ===================================================================
    // Binary data roundtrip
    // ===================================================================

    #[tokio::test]
    async fn realtime_publish_subscribe_binary() -> Result<()> {
        let app = TestApp::create().await?;
        let client = app.realtime_client();

        client
            .connection
            .wait_for_state_with_timeout(ConnectionState::Connected, STATE_TIMEOUT)
            .await?;

        let channel = client.channels.get("test_binary_pubsub").await;
        let mut sub = channel.subscribe().await?;

        let payload = serde_bytes::ByteBuf::from(vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD]);
        channel
            .publish(Some("binary"), Data::Binary(payload.clone()))
            .await?;

        let msg = tokio::time::timeout(MSG_TIMEOUT, sub.recv())
            .await
            .expect("Timed out waiting for binary message")
            .expect("Subscription closed");

        assert_eq!(msg.name, Some("binary".to_string()));
        assert_eq!(msg.data, Data::Binary(payload));

        client.close().await;
        Ok(())
    }
}
