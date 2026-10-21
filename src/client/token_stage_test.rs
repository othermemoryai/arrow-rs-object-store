// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::ClientOptions;
#[cfg(feature = "gcp-base")]
use crate::ObjectStoreExt;
use crate::client::ReadStage;
use crate::client::mock_server::MockServer;
use crate::client::{
    HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use async_trait::async_trait;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Notify;

#[derive(Debug)]
struct Completion(u64);
impl ReadStageCompletion for Completion {}

#[derive(Debug, Default)]
struct Observed {
    started: AtomicU64,
    body_settled: AtomicBool,
    omit_completion: AtomicBool,
    joins: Mutex<Vec<(u64, Option<u64>)>>,
    predecessors: Mutex<Vec<(u64, Vec<u64>)>>,
    finished: Mutex<Vec<(u64, bool)>>,
}

#[derive(Debug)]
struct Stage {
    id: u64,
    observed: Arc<Observed>,
    valid: AtomicBool,
}

impl ReadStage for Stage {
    fn span(&self) -> tracing::Span {
        tracing::Span::none()
    }

    fn join(&self, predecessor: Option<Arc<dyn ReadStageCompletion>>) -> bool {
        let id = predecessor.as_ref().and_then(|token| {
            (token.as_ref() as &dyn std::any::Any)
                .downcast_ref::<Completion>()
                .map(|token| token.0)
        });
        let mut joins = self.observed.joins.lock().unwrap();
        assert!(joins.len() < 16);
        joins.push((self.id, id));
        if id.is_none() {
            self.valid.store(false, Ordering::Release);
        }
        id.is_some()
    }

    fn finish(&self, succeeded: bool) -> Option<Arc<dyn ReadStageCompletion>> {
        if succeeded {
            assert!(self.observed.body_settled.load(Ordering::Acquire));
        }
        let mut finished = self.observed.finished.lock().unwrap();
        assert!(finished.len() < 16);
        finished.push((self.id, succeeded));
        (succeeded
            && self.valid.load(Ordering::Acquire)
            && !self.observed.omit_completion.load(Ordering::Acquire))
        .then(|| Arc::new(Completion(self.id)) as Arc<dyn ReadStageCompletion>)
    }
}

#[derive(Debug)]
struct Service {
    inner: HttpClient,
    observed: Arc<Observed>,
}

#[async_trait]
impl HttpService for Service {
    fn begin_read_stage(
        &self,
        predecessors: Vec<Arc<dyn ReadStageCompletion>>,
    ) -> Option<Arc<dyn ReadStage>> {
        let id = self.observed.started.fetch_add(1, Ordering::AcqRel) + 1;
        let parents = predecessors
            .into_iter()
            .map(|token| {
                (token.as_ref() as &dyn std::any::Any)
                    .downcast_ref::<Completion>()
                    .unwrap()
                    .0
            })
            .collect();
        self.observed
            .predecessors
            .lock()
            .unwrap()
            .push((id, parents));
        assert!(id <= 8);
        Some(Arc::new(Stage {
            id,
            observed: self.observed.clone(),
            valid: AtomicBool::new(true),
        }))
    }

    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        self.inner.execute(request).await
    }
}

fn client(observed: Arc<Observed>) -> HttpClient {
    let inner = ReqwestConnector::default()
        .connect(&ClientOptions::new().with_allow_http(true))
        .unwrap();
    HttpClient::new(Service { inner, observed })
}

async fn fetch(
    client: &HttpClient,
    url: &str,
    observed: &Observed,
) -> Result<TemporaryToken<String>, String> {
    let request = http::Request::builder()
        .uri(url)
        .body(String::new().into())
        .unwrap();
    let body = client
        .execute(request)
        .await
        .map_err(|error| error.to_string())?
        .into_body()
        .text()
        .await
        .map_err(|error| error.to_string())?;
    if body != "credential" {
        return Err("credential body rejected".into());
    }
    observed.body_settled.store(true, Ordering::Release);
    Ok(TemporaryToken {
        token: body,
        expiry: None,
    })
}

#[tokio::test]
async fn ready_credential_cache_hits_create_no_wait_stage() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = MockServer::new().await;
        server.push(http::Response::new("credential".to_string()));
        let observed = Arc::new(Observed::default());
        let client = client(observed.clone());
        let cache = TokenCache::default();
        let token = cache
            .get_or_insert_with_client(Some(&client), || fetch(&client, server.url(), &observed))
            .await
            .unwrap();
        assert_eq!(token.0, "credential");
        assert!(matches!(token.1, CredentialRead::Completed(_)));
        let cached = cache
            .get_or_insert_with_client(Some(&client), || async {
                panic!("ready credential cache fetched another token");
                #[allow(unreachable_code)]
                Ok::<_, String>(TemporaryToken {
                    token: String::new(),
                    expiry: None,
                })
            })
            .await
            .unwrap();
        assert_eq!(cached.0, token.0);
        assert!(matches!(cached.1, CredentialRead::Ready));
        assert_eq!(
            observed.started.load(Ordering::Acquire),
            1,
            "ready cache hit created a wait stage"
        );
        assert_eq!(*observed.finished.lock().unwrap(), [(1, true)]);
        assert!(observed.joins.lock().unwrap().is_empty());
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn blocked_credential_waiter_joins_the_published_completion() {
    for omit in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = MockServer::new().await;
            let arrived = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let entered = arrived.clone();
            let resume = release.clone();
            server.push_async_fn(move |_| async move {
                entered.notify_one();
                resume.notified().await;
                http::Response::new("credential".to_string())
            });
            let observed = Arc::new(Observed::default());
            observed.omit_completion.store(omit, Ordering::Release);
            let client = client(observed.clone());
            let cache = Arc::new(TokenCache::default());
            let producer_cache = cache.clone();
            let producer_client = client.clone();
            let producer_observed = observed.clone();
            let url = server.url().to_string();
            let producer = tokio::spawn(async move {
                producer_cache
                    .get_or_insert_with_client(Some(&producer_client), || {
                        fetch(&producer_client, &url, &producer_observed)
                    })
                    .await
            });
            arrived.notified().await;
            let waiter = cache.get_or_insert_with_client(Some(&client), || async {
                panic!("blocked waiter duplicated the credential fetch");
                #[allow(unreachable_code)]
                Ok::<_, String>(TemporaryToken {
                    token: String::new(),
                    expiry: None,
                })
            });
            futures_util::pin_mut!(waiter);
            assert!(futures_util::poll!(&mut waiter).is_pending());
            assert_eq!(observed.started.load(Ordering::Acquire), 2);
            assert!(observed.finished.lock().unwrap().is_empty());
            release.notify_one();
            assert_eq!(producer.await.unwrap().unwrap().0, "credential");
            let waited = waiter.await.unwrap();
            assert_eq!(waited.0, "credential");
            assert_eq!(matches!(waited.1, CredentialRead::Unavailable), omit);
            if let CredentialRead::Completed(token) = waited.1 {
                assert_eq!(
                    (token.as_ref() as &dyn std::any::Any)
                        .downcast_ref::<Completion>()
                        .unwrap()
                        .0,
                    2
                );
            }
            assert_eq!(
                *observed.joins.lock().unwrap(),
                [(2, (!omit).then_some(1))],
                "blocked credential waiter lost its producer join"
            );
            assert_eq!(*observed.finished.lock().unwrap(), [(1, true), (2, true)]);
            server.shutdown().await;
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn canceled_credential_refresh_finishes_without_a_token() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = MockServer::new().await;
        let arrived = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered = arrived.clone();
        let resume = release.clone();
        server.push_async_fn(move |_| async move {
            entered.notify_one();
            resume.notified().await;
            http::Response::new("credential".to_string())
        });
        let observed = Arc::new(Observed::default());
        let client = client(observed.clone());
        let cache = Arc::new(TokenCache::default());
        let producer_cache = cache.clone();
        let producer_observed = observed.clone();
        let url = server.url().to_string();
        let producer = tokio::spawn(async move {
            producer_cache
                .get_or_insert_with_client(Some(&client), || {
                    fetch(&client, &url, &producer_observed)
                })
                .await
        });
        arrived.notified().await;
        producer.abort();
        assert!(producer.await.unwrap_err().is_cancelled());
        assert!(cache.cache.try_read().unwrap().is_none());
        assert_eq!(
            *observed.finished.lock().unwrap(),
            [(1, false)],
            "canceled credential producer published a successful stage"
        );
        release.notify_one();
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "gcp-base")]
#[derive(Debug)]
struct CredentialSource {
    url: String,
    observed: Arc<Observed>,
}

#[cfg(feature = "gcp-base")]
#[async_trait]
impl crate::client::TokenProvider for CredentialSource {
    type Credential = crate::gcp::GcpCredential;

    async fn fetch_token(
        &self,
        client: &HttpClient,
        _: &crate::RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<Self::Credential>>> {
        let value = fetch(client, &self.url, &self.observed)
            .await
            .map_err(|error| crate::Error::Generic {
                store: "credential fixture",
                source: Box::new(std::io::Error::other(error)),
            })?;
        Ok(TemporaryToken {
            token: Arc::new(crate::gcp::GcpCredential {
                bearer: value.token,
            }),
            expiry: value.expiry,
        })
    }
}

#[cfg(feature = "gcp-base")]
#[derive(Debug)]
struct Connector(Arc<Observed>);

#[cfg(feature = "gcp-base")]
impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> crate::Result<HttpClient> {
        Ok(HttpClient::new(Service {
            inner: ReqwestConnector::default().connect(options)?,
            observed: self.0.clone(),
        }))
    }
}

#[cfg(feature = "gcp-base")]
fn gcs(server: &MockServer, observed: Arc<Observed>) -> crate::gcp::GoogleCloudStorage {
    let credentials = crate::client::TokenCredentialProvider::new(
        CredentialSource {
            url: format!("{}/token", server.url()),
            observed: observed.clone(),
        },
        client(observed.clone()),
        crate::RetryConfig::default(),
    );
    crate::gcp::GoogleCloudStorageBuilder::new()
        .with_bucket_name("bucket")
        .with_base_url(server.url())
        .with_credentials(Arc::new(credentials))
        .with_http_connector(Connector(observed))
        .build()
        .unwrap()
}

#[cfg(feature = "gcp-base")]
fn data_response() -> http::Response<String> {
    http::Response::builder()
        .header("etag", "fixture")
        .header("last-modified", "Wed, 21 Oct 2026 00:00:00 GMT")
        .header("content-length", "6")
        .body("object".to_string())
        .unwrap()
}

#[cfg(feature = "gcp-base")]
#[tokio::test]
async fn gcs_requests_follow_their_local_credential_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = MockServer::new().await;
        let arrived = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let heads = Arc::new(AtomicU64::new(0));
        let token_heads = heads.clone();
        let entered = arrived.clone();
        let resume = release.clone();
        server.push_async_fn(move |request| async move {
            token_heads.fetch_add(1, Ordering::AcqRel);
            assert_eq!(request.uri().path(), "/token");
            entered.notify_one();
            resume.notified().await;
            http::Response::new("credential".to_string())
        });
        for _ in 0..3 {
            let data_heads = heads.clone();
            server.push_fn(move |request| {
                data_heads.fetch_add(1, Ordering::AcqRel);
                assert_eq!(request.uri().path(), "/bucket/object");
                assert_eq!(request.headers()["authorization"], "Bearer credential");
                data_response()
            });
        }
        let observed = Arc::new(Observed::default());
        let gcs = Arc::new(gcs(&server, observed.clone()));
        let first_store = gcs.clone();
        let first =
            tokio::spawn(async move { first_store.get(&crate::path::Path::from("object")).await });
        arrived.notified().await;
        let path = crate::path::Path::from("object");
        let second = gcs.get(&path);
        futures_util::pin_mut!(second);
        assert!(futures_util::poll!(&mut second).is_pending());
        assert_eq!(observed.started.load(Ordering::Acquire), 2);
        release.notify_one();
        let (first, second) = futures_util::join!(first, second);
        let (first, second) =
            futures_util::join!(first.unwrap().unwrap().bytes(), second.unwrap().bytes());
        assert_eq!(heads.load(Ordering::Acquire), 3);
        assert_eq!(first.unwrap().as_ref(), b"object");
        assert_eq!(second.unwrap().as_ref(), b"object");
        assert_eq!(*observed.joins.lock().unwrap(), [(2, Some(1))]);
        let predecessors = observed.predecessors.lock().unwrap().clone();
        let mut data_predecessors: Vec<_> = predecessors
            .into_iter()
            .filter_map(|(_, parents)| parents.first().copied())
            .collect();
        data_predecessors.sort_unstable();
        assert_eq!(
            data_predecessors,
            [1, 2],
            "GCS data requests lost their local credential completion"
        );
        assert_eq!(observed.started.load(Ordering::Acquire), 4);
        assert_eq!(
            gcs.get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"object"
        );
        assert_eq!(
            observed.started.load(Ordering::Acquire),
            4,
            "ready credential created another data prerequisite"
        );
        assert_eq!(heads.load(Ordering::Acquire), 4);
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "gcp-base")]
#[tokio::test]
async fn gcs_data_stage_stays_open_until_body_terminal_or_drop() {
    use futures_util::StreamExt;
    for outcome in 0..3 {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = MockServer::new().await;
            server.push(http::Response::new("credential".to_string()));
            let release_error = Arc::new(Notify::new());
            if outcome == 2 {
                let release = release_error.clone();
                let frames = futures_util::stream::once(async {
                    Ok::<_, HttpError>(hyper::body::Frame::data(bytes::Bytes::from_static(b"obj")))
                })
                .chain(futures_util::stream::once(async move {
                    release.notified().await;
                    Err::<hyper::body::Frame<bytes::Bytes>, _>(HttpError::new(
                        crate::client::HttpErrorKind::Decode,
                        std::io::Error::other("fixture body failed after headers"),
                    ))
                }));
                let body =
                    crate::client::HttpResponseBody::new(http_body_util::StreamBody::new(frames));
                let (parts, _) = data_response().into_parts();
                server.push(http::Response::from_parts(parts, body));
            } else {
                server.push(data_response());
            }
            let observed = Arc::new(Observed::default());
            let gcs = gcs(&server, observed.clone());
            let result = gcs.get(&crate::path::Path::from("object")).await.unwrap();
            assert_eq!(
                *observed.finished.lock().unwrap(),
                [(1, true)],
                "data stage closed before body terminal"
            );
            match outcome {
                0 => assert_eq!(result.bytes().await.unwrap().as_ref(), b"object"),
                1 => drop(result),
                _ => {
                    release_error.notify_one();
                    assert!(
                        result.bytes().await.is_err(),
                        "fixture body error disappeared after headers"
                    );
                }
            }
            assert_eq!(
                *observed.finished.lock().unwrap(),
                [(1, true), (2, outcome == 0)],
                "data stage changed its body outcome"
            );
            server.shutdown().await;
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn failed_credential_refresh_does_not_publish_completion() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = MockServer::new().await;
        server.push(http::Response::new("invalid".to_string()));
        let observed = Arc::new(Observed::default());
        let client = client(observed.clone());
        let cache = TokenCache::default();
        let result = cache
            .get_or_insert_with_client(Some(&client), || fetch(&client, server.url(), &observed))
            .await;
        assert_eq!(result.unwrap_err(), "credential body rejected");
        assert!(cache.cache.try_read().unwrap().is_none());
        assert_eq!(
            *observed.finished.lock().unwrap(),
            [(1, false)],
            "failed credential producer published a successful stage"
        );
        server.shutdown().await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "gcp-base")]
#[derive(Debug)]
struct UnsupportedCredential;

#[cfg(feature = "gcp-base")]
#[async_trait]
impl crate::CredentialProvider for UnsupportedCredential {
    type Credential = crate::gcp::GcpCredential;

    async fn get_credential(&self) -> crate::Result<Arc<Self::Credential>> {
        Ok(Arc::new(crate::gcp::GcpCredential {
            bearer: "credential".into(),
        }))
    }
}

#[cfg(feature = "gcp-base")]
#[tokio::test]
async fn unsupported_credentials_remain_distinct_from_static_ready_values() {
    for ready in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = MockServer::new().await;
            server.push(data_response());
            let observed = Arc::new(Observed::default());
            observed.body_settled.store(true, Ordering::Release);
            let provider: crate::gcp::GcpCredentialProvider = if ready {
                Arc::new(crate::StaticCredentialProvider::new(
                    crate::gcp::GcpCredential {
                        bearer: "credential".into(),
                    },
                ))
            } else {
                Arc::new(UnsupportedCredential)
            };
            let gcs = crate::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name("bucket")
                .with_base_url(server.url())
                .with_credentials(provider)
                .with_http_connector(Connector(observed.clone()))
                .build()
                .unwrap();
            assert_eq!(
                gcs.get(&crate::path::Path::from("object"))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .as_ref(),
                b"object"
            );
            assert_eq!(
                observed.started.load(Ordering::Acquire),
                if ready { 0 } else { 1 }
            );
            assert_eq!(
                *observed.joins.lock().unwrap(),
                if ready { vec![] } else { vec![(1, None)] },
                "unsupported provider silently became a ready cache hit"
            );
            server.shutdown().await;
        })
        .await
        .unwrap();
    }
}

#[cfg(feature = "gcp-base")]
#[tokio::test]
async fn gcs_head_and_list_forward_the_credential_completion() {
    use crate::ObjectStore;
    use futures_util::TryStreamExt;
    for head in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = MockServer::new().await;
            server.push(http::Response::new("credential".to_string()));
            server.push_fn(move |request| {
                if head {
                    assert_eq!(request.method(), http::Method::HEAD);
                    data_response()
                } else {
                    assert_eq!(request.uri().query(), Some("list-type=2"));
                    http::Response::new("<ListBucketResult/>".to_string())
                }
            });
            let observed = Arc::new(Observed::default());
            let gcs = gcs(&server, observed.clone());
            if head {
                let result = gcs.head(&crate::path::Path::from("object")).await.unwrap();
                assert_eq!(result.size, 6);
            } else {
                assert!(
                    gcs.list(None)
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            assert_eq!(
                *observed.predecessors.lock().unwrap(),
                [(1, vec![]), (2, vec![1])],
                "GCS metadata request lost its credential completion"
            );
            assert_eq!(*observed.finished.lock().unwrap(), [(1, true), (2, true)]);
            server.shutdown().await;
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn refresh_after_a_blocked_wait_starts_after_the_local_waiter() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let server = MockServer::new().await;
        let arrived = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered = arrived.clone();
        let resume = release.clone();
        server.push_async_fn(move |_| async move {
            entered.notify_one();
            resume.notified().await;
            http::Response::new("credential".to_string())
        });
        server.push(http::Response::new("credential".to_string()));
        let observed = Arc::new(Observed::default());
        let client = client(observed.clone());
        let cache = Arc::new(TokenCache::default());
        let producer_cache = cache.clone();
        let producer_client = client.clone();
        let producer_observed = observed.clone();
        let url = server.url().to_string();
        let first = tokio::spawn(async move {
            producer_cache
                .get_or_insert_with_client(Some(&producer_client), || async {
                    let mut token = fetch(&producer_client, &url, &producer_observed).await?;
                    token.expiry = Some(Instant::now());
                    Ok::<_, String>(token)
                })
                .await
        });
        arrived.notified().await;
        let second_url = server.url().to_string();
        let second = cache
            .get_or_insert_with_client(Some(&client), || fetch(&client, &second_url, &observed));
        futures_util::pin_mut!(second);
        assert!(futures_util::poll!(&mut second).is_pending());
        release.notify_one();
        assert_eq!(first.await.unwrap().unwrap().0, "credential");
        assert_eq!(second.await.unwrap().0, "credential");
        assert_eq!(*observed.joins.lock().unwrap(), [(2, Some(1))]);
        assert_eq!(
            *observed.predecessors.lock().unwrap(),
            [(1, vec![]), (2, vec![]), (3, vec![2])],
            "credential refresh after a lock wait lost its local prerequisite"
        );
        assert_eq!(
            *observed.finished.lock().unwrap(),
            [(1, true), (2, true), (3, true)]
        );
        server.shutdown().await;
    })
    .await
    .unwrap();
}
