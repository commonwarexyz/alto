use crate::Source;
use alto_client::{consensus::Payload, IndexQuery, Query};
use bytes::{Buf, Bytes};
use commonware_actor::{Feedback, Unreliable};
use commonware_codec::{Encode, ReadExt, Write};
use commonware_consensus::marshal::resolver::{
    handler,
    p2p::{self as marshal_p2p, Mailbox},
};
use commonware_cryptography::{
    ed25519::{PrivateKey, PublicKey},
    sha256::Digest,
    Signer,
};
use commonware_p2p::{
    utils::StaticProvider, Blocker, CheckedSender, LimitedSender, Message, Receiver, Recipients,
};
use commonware_runtime::{
    spawn_cell, BufferPooler, Clock, ContextCell, Handle, IoBuf, IoBufs, Metrics, Spawner,
};
use commonware_utils::{
    channel::{fallible::AsyncFallibleExt as _, mpsc},
    ordered::Set,
};
use rand::Rng;
use std::{
    fmt, io,
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};
use tracing::{debug, warn};

pub type Resolver = Mailbox<Digest, PublicKey>;

struct Request {
    id: u64,
    key: handler::Key<Digest>,
}

struct SourceActor<E: Clock + Spawner, C: Source> {
    context: ContextCell<E>,
    client: C,
    requests: mpsc::Receiver<Request>,
    responses: mpsc::Sender<Message<PublicKey>>,
    peer: PublicKey,
}

impl<E: Clock + Spawner, C: Source> SourceActor<E, C> {
    fn new(
        context: E,
        client: C,
        requests: mpsc::Receiver<Request>,
        responses: mpsc::Sender<Message<PublicKey>>,
        peer: PublicKey,
    ) -> Self {
        Self {
            context: ContextCell::new(context),
            client,
            requests,
            responses,
            peer,
        }
    }

    fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        while let Some(request) = self.requests.recv().await {
            let response = match fetch(&self.client, request.key).await {
                Some(value) => encode_response(request.id, Some(value)),
                None => encode_response(request.id, None),
            };
            let _ = self
                .responses
                .send((self.peer.clone(), IoBuf::from(response)))
                .await;
        }
    }
}

#[derive(Clone)]
struct SourceSender {
    requests: mpsc::Sender<Request>,
    peer: PublicKey,
}

struct SourceCheckedSender {
    requests: mpsc::Sender<Request>,
    peer: PublicKey,
}

impl LimitedSender for SourceSender {
    type PublicKey = PublicKey;
    type Checked<'a> = SourceCheckedSender;

    fn check(
        &mut self,
        recipients: Recipients<Self::PublicKey>,
    ) -> Result<Self::Checked<'_>, SystemTime> {
        let selected = match recipients {
            Recipients::All => true,
            Recipients::Some(peers) => peers.contains(&self.peer),
            Recipients::One(peer) => peer == self.peer,
        };
        selected
            .then(|| SourceCheckedSender {
                requests: self.requests.clone(),
                peer: self.peer.clone(),
            })
            .ok_or_else(SystemTime::now)
    }
}

impl CheckedSender for SourceCheckedSender {
    type PublicKey = PublicKey;

    fn recipients(&self) -> Vec<Self::PublicKey> {
        vec![self.peer.clone()]
    }

    fn send(self, message: impl Into<IoBufs> + Send, _priority: bool) -> Unreliable<Feedback> {
        let Some(request) = decode_request(message) else {
            return Unreliable::Rejected;
        };
        if self.requests.try_send_lossy(request) {
            Unreliable::new(Feedback::Ok)
        } else {
            Unreliable::Rejected
        }
    }
}

struct SourceReceiver {
    responses: mpsc::Receiver<Message<PublicKey>>,
}

impl fmt::Debug for SourceReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceReceiver").finish_non_exhaustive()
    }
}

impl Receiver for SourceReceiver {
    type Error = io::Error;
    type PublicKey = PublicKey;

    async fn recv(&mut self) -> Result<Message<Self::PublicKey>, Self::Error> {
        self.responses.recv().await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source response channel closed",
            )
        })
    }
}

#[derive(Clone)]
struct SourceBlocker;

impl Blocker for SourceBlocker {
    type PublicKey = PublicKey;

    fn block(&mut self, peer: Self::PublicKey) -> Feedback {
        warn!(?peer, "source-backed resolver peer blocked");
        Feedback::Ok
    }
}

pub fn init<E, C>(
    context: E,
    client: C,
    mailbox_size: NonZeroUsize,
    fetch_retry_timeout: Duration,
) -> (handler::Receiver<Digest>, Resolver)
where
    E: BufferPooler + Rng + Spawner + Clock + Metrics,
    C: Source,
{
    let local = PrivateKey::from_seed(0).public_key();
    let peer = PrivateKey::from_seed(1).public_key();
    let (requests_tx, requests_rx) = mpsc::channel(mailbox_size.get());
    let (responses_tx, responses_rx) = mpsc::channel(mailbox_size.get());

    SourceActor::new(
        context.child("source"),
        client,
        requests_rx,
        responses_tx,
        peer.clone(),
    )
    .start();

    marshal_p2p::init(
        context.child("resolver"),
        marshal_p2p::Config {
            public_key: local,
            peer_provider: StaticProvider::new(0, Set::from_iter_dedup([peer.clone()])),
            blocker: SourceBlocker,
            mailbox_size,
            initial: fetch_retry_timeout,
            timeout: fetch_retry_timeout,
            fetch_retry_timeout,
            priority_requests: false,
            priority_responses: false,
        },
        (
            SourceSender {
                requests: requests_tx,
                peer: peer.clone(),
            },
            SourceReceiver {
                responses: responses_rx,
            },
        ),
    )
}

fn decode_request(message: impl Into<IoBufs>) -> Option<Request> {
    let mut message = message.into();
    let mut bytes = message.copy_to_bytes(message.remaining());
    let id = u64::read(&mut bytes).ok()?;
    let payload = u8::read(&mut bytes).ok()?;
    if payload != 0 {
        return None;
    }
    let key = handler::Key::<Digest>::read(&mut bytes).ok()?;
    Some(Request { id, key })
}

fn encode_response(id: u64, value: Option<Bytes>) -> Vec<u8> {
    let mut encoded = Vec::new();
    id.write(&mut encoded);
    match value {
        Some(value) => {
            1u8.write(&mut encoded);
            value.write(&mut encoded);
        }
        None => 2u8.write(&mut encoded),
    }
    encoded
}

async fn fetch(client: &impl Source, key: handler::Key<Digest>) -> Option<Bytes> {
    match key {
        handler::Key::Block(digest) => fetch_block_by_digest(client, digest).await,
        handler::Key::Finalized { height } => fetch_finalized_by_height(client, height).await,
        handler::Key::Notarized { round } => fetch_notarized_by_round(client, round).await,
    }
}

async fn fetch_block_by_digest(client: &impl Source, digest: Digest) -> Option<Bytes> {
    debug!(?digest, "fetching block by digest");
    match client.block(Query::Digest(digest)).await {
        Ok(Payload::Block(block)) => Some(Bytes::from(block.encode().to_vec())),
        Ok(_) => {
            warn!(?digest, "wrong payload returned for block by digest");
            None
        }
        Err(e) => {
            warn!(?digest, error = ?e, "failed to fetch block by digest");
            None
        }
    }
}

async fn fetch_finalized_by_height(
    client: &impl Source,
    height: commonware_consensus::types::Height,
) -> Option<Bytes> {
    debug!(height = height.get(), "fetching finalized block by height");
    match client.block(Query::Index(height.get())).await {
        Ok(Payload::Finalized(finalized)) => Some(Bytes::from(
            (finalized.proof.clone(), finalized.block.clone())
                .encode()
                .to_vec(),
        )),
        Ok(_) => {
            warn!(
                height = height.get(),
                "wrong payload returned for finalized block by height"
            );
            None
        }
        Err(e) => {
            warn!(height = height.get(), error = ?e, "failed to fetch finalized block by height");
            None
        }
    }
}

async fn fetch_notarized_by_round(
    client: &impl Source,
    round: commonware_consensus::types::Round,
) -> Option<Bytes> {
    let view = round.view().get();
    debug!(view, "fetching notarized block by round");
    match client.notarized(IndexQuery::Index(view)).await {
        Ok(notarized) => Some(Bytes::from(
            (notarized.proof.clone(), notarized.block.clone())
                .encode()
                .to_vec(),
        )),
        Err(e) => {
            warn!(view, error = ?e, "failed to fetch notarized block by round");
            None
        }
    }
}
