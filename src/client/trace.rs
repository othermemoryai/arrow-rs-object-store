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

use super::{
    CredentialRead, HttpClient, HttpError, HttpResponse, HttpResponseBody, ReadStage,
    ReadStageCompletion,
};
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::{Instrument, Span};

pub(super) struct ReadScope {
    parent: Span,
    span: Span,
}

impl ReadScope {
    pub(super) fn new(span: Span) -> Self {
        let parent = Span::current();
        dependency(&parent, 1, &span, 1);
        Self { parent, span }
    }

    pub(super) fn span(&self) -> &Span {
        &self.span
    }

    pub(super) fn response(self, response: HttpResponse) -> HttpResponse {
        let (parts, body) = response.into_parts();
        let body = if body.is_end_stream() {
            drop(self);
            body
        } else {
            HttpResponseBody::new(ScopedBody {
                inner: body,
                scope: Some(self),
                stage: None,
            })
        };
        HttpResponse::from_parts(parts, body)
    }
}

impl Drop for ReadScope {
    fn drop(&mut self) {
        dependency(&self.span, 2, &self.parent, 2);
    }
}

fn dependency(
    predecessor: &Span,
    predecessor_endpoint: u64,
    dependent: &Span,
    dependent_endpoint: u64,
) {
    let left = predecessor.with_subscriber(|(id, dispatch)| (id.into_u64(), dispatch.clone()));
    let right = dependent.with_subscriber(|(id, dispatch)| (id.into_u64(), dispatch.clone()));
    let (predecessor, dependent, dispatch) = match (left, right) {
        (Some((left, dispatch)), Some((right, other))) if dispatch.is_same(&other) => {
            (left, right, dispatch)
        }
        (Some(_), Some((right, dispatch))) => (0, right, dispatch),
        (Some((left, dispatch)), None) => (left, 0, dispatch),
        (None, Some((right, dispatch))) => (0, right, dispatch),
        (None, None) => return,
    };
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::event!(name: "object_store.read.dependency", tracing::Level::INFO,
            predecessor, predecessor_endpoint, dependent, dependent_endpoint);
    });
}

pub(crate) struct ReadStageScope(Option<Arc<dyn ReadStage>>);

impl ReadStageScope {
    pub(crate) fn new(
        client: Option<&HttpClient>,
        predecessors: Vec<Arc<dyn ReadStageCompletion>>,
    ) -> Self {
        Self(client.and_then(|client| client.begin_read_stage(predecessors)))
    }

    pub(crate) fn after(client: &HttpClient, predecessor: CredentialRead) -> Self {
        match predecessor {
            CredentialRead::Completed(predecessor) => Self::new(Some(client), vec![predecessor]),
            CredentialRead::Ready => Self(None),
            CredentialRead::Unavailable => {
                let stage = Self::new(Some(client), Vec::new());
                stage.join(None);
                stage
            }
        }
    }

    pub(crate) async fn run<F: Future>(&self, future: F) -> F::Output {
        match &self.0 {
            Some(stage) => future.instrument(stage.span()).await,
            None => future.await,
        }
    }

    pub(crate) fn join(&self, completion: Option<Arc<dyn ReadStageCompletion>>) {
        if let Some(stage) = &self.0 {
            stage.join(completion);
        }
    }

    pub(crate) fn finish(mut self, succeeded: bool) -> Option<Arc<dyn ReadStageCompletion>> {
        self.0.take().and_then(|stage| stage.finish(succeeded))
    }

    pub(crate) fn response(self, response: HttpResponse) -> HttpResponse {
        if self.0.is_none() {
            return response;
        }
        let (parts, body) = response.into_parts();
        let body = if body.is_end_stream() {
            self.finish(true);
            body
        } else {
            HttpResponseBody::new(ScopedBody {
                inner: body,
                scope: None,
                stage: Some(self),
            })
        };
        HttpResponse::from_parts(parts, body)
    }
}

impl Drop for ReadStageScope {
    fn drop(&mut self) {
        if let Some(stage) = self.0.take() {
            stage.finish(false);
        }
    }
}

struct ScopedBody {
    inner: HttpResponseBody,
    scope: Option<ReadScope>,
    stage: Option<ReadStageScope>,
}

impl Body for ScopedBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(&frame, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
            || self.inner.is_end_stream()
        {
            drop(self.scope.take());
            if let Some(stage) = self.stage.take() {
                stage.finish(!matches!(&frame, Poll::Ready(Some(Err(_)))));
            }
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
