//! Client for interacting with `alto`.

use alto_types::{Identity, Scheme};
use commonware_cryptography::sha256::Digest;
use commonware_utils::hex;
use thiserror::Error;

pub mod consensus;
pub mod utils;

const LATEST: &str = "latest";

pub enum Query {
    Latest,
    Index(u64),
    Digest(Digest),
}

impl Query {
    pub fn serialize(&self) -> String {
        match self {
            Query::Latest => LATEST.to_string(),
            Query::Index(index) => hex(&index.to_be_bytes()),
            Query::Digest(digest) => hex(digest),
        }
    }
}

pub enum IndexQuery {
    Latest,
    Index(u64),
}

impl IndexQuery {
    pub fn serialize(&self) -> String {
        match self {
            IndexQuery::Latest => LATEST.to_string(),
            IndexQuery::Index(index) => hex(&index.to_be_bytes()),
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("tungstenite error: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("failed: {0}")]
    Failed(reqwest::StatusCode),
    #[error("invalid data: {0}")]
    InvalidData(#[from] commonware_codec::Error),
    #[error("invalid signature")]
    InvalidSignature,
    #[error("unexpected response")]
    UnexpectedResponse,
}

#[derive(Clone)]
pub struct Client {
    uri: String,
    ws_uri: String,
    certificate_verifier: Scheme,

    client: reqwest::Client,
}

impl Client {
    pub fn new(uri: &str, identity: Identity) -> Self {
        let uri = uri.to_string();
        let ws_uri = uri.replace("http", "ws");
        let certificate_verifier = Scheme::certificate_verifier(identity);
        Self {
            uri,
            ws_uri,
            certificate_verifier,

            client: reqwest::Client::new(),
        }
    }
}
