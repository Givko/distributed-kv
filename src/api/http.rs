//! Client-facing HTTP API for the key-value store.
//!
//! Both handlers are thin adapters over the node's mailbox: they build a
//! `RaftMsg`, send it, and await the reply on a oneshot channel. All consensus
//! logic lives in the node; nothing here touches Raft state directly.

use crate::raft::raft_types::{ChangeStateReply, RaftMsg};
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

#[derive(Serialize)]
pub struct GetResponse {
    pub value: String,
}

#[derive(Deserialize)]
pub struct SetRequest {
    pub value: String,
}

#[derive(Serialize)]
pub struct SetResponse {
    pub success: bool,
    pub leader: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    /// Set when the node knows who the leader is, so the caller can retry there.
    #[serde(skip_serializing_if = "String::is_empty")]
    leader: String,
}

/// Handler failures. The node itself never returns errors to a client; these
/// cover the mailbox or the reply channel going away, plus writes that reached
/// a follower.
enum ApiError {
    NotLeader { leader: String },
    NodeUnavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error, leader) = match self {
            // 503 rather than a redirect: the caller must re-issue the write,
            // and the leader may have changed again by the time they do.
            ApiError::NotLeader { leader } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "node is not leader".to_string(),
                leader,
            ),
            ApiError::NodeUnavailable => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "node is not accepting messages".to_string(),
                String::new(),
            ),
        };
        (status, axum::Json(ErrorResponse { error, leader })).into_response()
    }
}

pub fn router(mailbox: Sender<RaftMsg>) -> Router {
    Router::new()
        .route("/kv/{key}", get(get_key).put(set_key))
        .with_state(mailbox)
}

pub async fn serve(addr: std::net::SocketAddr, mailbox: Sender<RaftMsg>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("HTTP API listening on {addr}");
    axum::serve(listener, router(mailbox)).await?;
    Ok(())
}

async fn get_key(
    State(mailbox): State<Sender<RaftMsg>>,
    Path(key): Path<String>,
) -> Result<axum::Json<GetResponse>, ApiError> {
    let (snd, rcv) = tokio::sync::oneshot::channel::<String>();
    mailbox
        .send(RaftMsg::GetState {
            key,
            reply_channel: snd,
        })
        .await
        .map_err(|_| ApiError::NodeUnavailable)?;

    let value = rcv.await.map_err(|_| ApiError::NodeUnavailable)?;
    Ok(axum::Json(GetResponse { value }))
}

async fn set_key(
    State(mailbox): State<Sender<RaftMsg>>,
    Path(key): Path<String>,
    axum::Json(body): axum::Json<SetRequest>,
) -> Result<axum::Json<SetResponse>, ApiError> {
    let command = format!("SET {} {}", key, body.value);
    let (snd, rcv) = tokio::sync::oneshot::channel::<ChangeStateReply>();
    mailbox
        .send(RaftMsg::ChangeState {
            command,
            reply_channel: Some(snd),
        })
        .await
        .map_err(|_| ApiError::NodeUnavailable)?;

    let reply: ChangeStateReply = rcv.await.map_err(|_| ApiError::NodeUnavailable)?;
    if !reply.success {
        return Err(ApiError::NotLeader {
            leader: reply.leader,
        });
    }
    Ok(axum::Json(SetResponse {
        success: true,
        leader: reply.leader,
    }))
}
