use crate::Source;
use alto_client::{consensus::Payload, IndexQuery, Query};
use bytes::Bytes;
use commonware_actor::{mailbox, Feedback};
use commonware_codec::Encode;
use commonware_consensus::{
    marshal::resolver::handler,
    types::{Height, Round},
};
use commonware_cryptography::{ed25519::PublicKey, sha256::Digest};
use commonware_macros::select_loop;
use commonware_resolver::{Consumer as _, Delivery, Fetch};
use commonware_runtime::{spawn_cell, Clock, ContextCell, Handle, Metrics, Spawner};
use commonware_utils::{
    futures::{AbortablePool, Aborter},
    sync::Mutex,
    vec::NonEmptyVec,
};
use futures::future::{self, Either};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::{debug, trace, warn};

type Key = handler::Key<Digest>;
type Subscriber = handler::Annotation;
type FetchRequest = Fetch<Key, Subscriber>;
type Subscribers = Arc<Mutex<Vec<Subscriber>>>;

/// Handle to the source-backed resolver actor used by marshal.
#[derive(Clone)]
pub struct Resolver {
    mailbox: mailbox::Sender<Message>,
}

impl commonware_resolver::Resolver for Resolver {
    type Key = Key;
    type Subscriber = Subscriber;
    type PublicKey = PublicKey;

    fn fetch<F>(&mut self, fetch: F) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.send(Message::Fetch(fetch.into()))
    }

    fn fetch_all<F>(&mut self, fetches: Vec<F>) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.send(Message::FetchAll(
            fetches.into_iter().map(Into::into).collect(),
        ))
    }

    fn fetch_targeted(
        &mut self,
        fetch: impl Into<Fetch<Self::Key, Self::Subscriber>> + Send,
        _targets: NonEmptyVec<Self::PublicKey>,
    ) -> Feedback {
        self.fetch(fetch)
    }

    fn fetch_all_targeted<F>(&mut self, fetches: Vec<(F, NonEmptyVec<Self::PublicKey>)>) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.fetch_all(fetches.into_iter().map(|(fetch, _)| fetch).collect())
    }

    fn retain(
        &mut self,
        predicate: impl Fn(&Self::Key, &Self::Subscriber) -> bool + Send + 'static,
    ) -> Feedback {
        self.send(Message::Retain(Box::new(predicate)))
    }
}

impl Resolver {
    fn send(&self, message: Message) -> Feedback {
        self.mailbox.enqueue(message)
    }
}

enum Message {
    Fetch(FetchRequest),
    FetchAll(Vec<FetchRequest>),
    Retain(Box<dyn Fn(&Key, &Subscriber) -> bool + Send>),
}

impl mailbox::Policy for Message {
    type Overflow = VecDeque<Self>;

    fn handle(overflow: &mut Self::Overflow, message: Self) {
        overflow.push_back(message);
    }
}

struct Actor<E: Clock + Spawner, C: Source> {
    context: ContextCell<E>,
    client: C,
    mailbox: mailbox::Receiver<Message>,
    handler: handler::Handler<Digest>,
    active: AbortablePool<Result>,
    requests: BTreeMap<Key, RequestState>,
    retry_schedule: BTreeSet<(SystemTime, Key)>,
    fetch_retry_timeout: Duration,
    next_id: u64,
}

struct RequestState {
    subscribers: Subscribers,
    attempt: Attempt,
}

enum Attempt {
    Active { id: u64, _aborter: Aborter },
    Scheduled(SystemTime),
}

struct Result {
    key: Key,
    id: u64,
    retry: bool,
}

pub fn init<E, C>(
    context: E,
    client: C,
    mailbox_size: NonZeroUsize,
    fetch_retry_timeout: Duration,
) -> (handler::Receiver<Digest>, Resolver)
where
    E: Clock + Spawner + Metrics,
    C: Source,
{
    let (handler_rx, handler) = handler::init(context.child("handler"), mailbox_size);
    let (mailbox_tx, mailbox_rx) = mailbox::new(context.child("mailbox"), mailbox_size);
    Actor::new(
        context.child("actor"),
        client,
        mailbox_rx,
        handler,
        fetch_retry_timeout,
    )
    .start();
    (
        handler_rx,
        Resolver {
            mailbox: mailbox_tx,
        },
    )
}

impl<E, C> Actor<E, C>
where
    E: Clock + Spawner,
    C: Source,
{
    fn new(
        context: E,
        client: C,
        mailbox: mailbox::Receiver<Message>,
        handler: handler::Handler<Digest>,
        fetch_retry_timeout: Duration,
    ) -> Self {
        Self {
            context: ContextCell::new(context),
            client,
            mailbox,
            handler,
            active: AbortablePool::default(),
            requests: BTreeMap::new(),
            retry_schedule: BTreeSet::new(),
            fetch_retry_timeout,
            next_id: 0,
        }
    }

    fn start(mut self) -> Handle<()> {
        spawn_cell!(self.context, self.run())
    }

    async fn run(mut self) {
        select_loop! {
            self.context,
            on_stopped => {},
            Ok(result) = self.active.next_completed() else continue => {
                self.handle_completed(result);
            },
            _ = match self.retry_schedule.first() {
                Some((deadline, _)) => Either::Left(self.context.sleep_until(*deadline)),
                None => Either::Right(future::pending()),
            } => {
                self.process_retries();
            },
            Some(message) = self.mailbox.recv() else break => {
                self.handle_message(message);
            },
        }
    }

    fn handle_message(&mut self, message: Message) {
        match message {
            Message::Fetch(fetch) => self.add_fetch(fetch),
            Message::FetchAll(fetches) => {
                for fetch in fetches {
                    self.add_fetch(fetch);
                }
            }
            Message::Retain(predicate) => self.retain(predicate),
        }
    }

    fn add_fetch(&mut self, fetch: FetchRequest) {
        let Fetch { key, subscriber } = fetch;
        if let Some(state) = self.requests.get_mut(&key) {
            let mut subscribers = state.subscribers.lock();
            if !subscribers.contains(&subscriber) {
                subscribers.push(subscriber);
            }
            return;
        }

        let subscribers = Arc::new(Mutex::new(vec![subscriber]));
        self.requests.insert(
            key,
            RequestState {
                subscribers,
                attempt: Attempt::Scheduled(self.context.current()),
            },
        );
        self.start_fetch(key);
    }

    fn retain(&mut self, predicate: Box<dyn Fn(&Key, &Subscriber) -> bool + Send>) {
        let mut retained = Vec::new();
        for (key, state) in &mut self.requests {
            let mut subscribers = state.subscribers.lock();
            subscribers.retain(|subscriber| predicate(key, subscriber));
            if !subscribers.is_empty() {
                retained.push(*key);
            }
        }

        let retained = retained.into_iter().collect::<BTreeSet<_>>();
        let removed = self
            .requests
            .keys()
            .filter(|key| !retained.contains(key))
            .copied()
            .collect::<Vec<_>>();
        for key in removed {
            if let Some(state) = self.requests.remove(&key) {
                if let Attempt::Scheduled(deadline) = state.attempt {
                    self.retry_schedule.remove(&(deadline, key));
                }
            }
        }
    }

    fn start_fetch(&mut self, key: Key) {
        let subscribers = self
            .requests
            .get(&key)
            .expect("request missing")
            .subscribers
            .clone();
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let future = Self::process_fetch(
            key,
            id,
            self.client.clone(),
            self.handler.clone(),
            subscribers,
        );
        let aborter = self.active.push(future);
        let state = self.requests.get_mut(&key).expect("request missing");
        state.attempt = Attempt::Active {
            id,
            _aborter: aborter,
        };
    }

    fn handle_completed(&mut self, result: Result) {
        let Some(state) = self.requests.get(&result.key) else {
            trace!(?result.key, id = result.id, "ignoring stale fetch completion");
            return;
        };
        match state.attempt {
            Attempt::Active { id, .. } if id == result.id => {}
            Attempt::Active { id, .. } => {
                trace!(
                    ?result.key,
                    completed_id = result.id,
                    active_id = id,
                    "ignoring replaced fetch completion"
                );
                return;
            }
            Attempt::Scheduled(deadline) => {
                trace!(
                    ?result.key,
                    id = result.id,
                    ?deadline,
                    "ignoring scheduled fetch completion"
                );
                return;
            }
        }

        if result.retry {
            self.schedule_retry(result.key);
        } else {
            self.requests.remove(&result.key);
        }
    }

    fn schedule_retry(&mut self, key: Key) {
        let deadline = self.context.current() + self.fetch_retry_timeout;
        let Some(state) = self.requests.get_mut(&key) else {
            return;
        };
        state.attempt = Attempt::Scheduled(deadline);
        self.retry_schedule.insert((deadline, key));
        debug!(?key, ?deadline, "scheduled source resolver retry");
    }

    fn process_retries(&mut self) {
        let now = self.context.current();
        while let Some((deadline, key)) = self.retry_schedule.pop_first() {
            if deadline > now {
                self.retry_schedule.insert((deadline, key));
                break;
            }

            let Some(state) = self.requests.get(&key) else {
                continue;
            };
            match state.attempt {
                Attempt::Scheduled(state_deadline) if state_deadline == deadline => {
                    debug!(?key, "retrying source resolver fetch");
                    self.start_fetch(key);
                }
                Attempt::Scheduled(_) | Attempt::Active { .. } => {}
            }
        }
    }

    async fn process_fetch(
        key: Key,
        id: u64,
        client: C,
        handler: handler::Handler<Digest>,
        subscribers: Subscribers,
    ) -> Result {
        let retry = match key {
            handler::Key::Block(digest) => {
                Self::fetch_block_by_digest(key, digest, client, handler, subscribers).await
            }
            handler::Key::Finalized { height } => {
                Self::fetch_finalized_by_height(key, height, client, handler, subscribers).await
            }
            handler::Key::Notarized { round } => {
                Self::fetch_notarized_by_round(key, round, client, handler, subscribers).await
            }
        };
        Result { key, id, retry }
    }

    async fn fetch_block_by_digest(
        key: Key,
        digest: Digest,
        client: C,
        handler: handler::Handler<Digest>,
        subscribers: Subscribers,
    ) -> bool {
        debug!(?digest, "fetching block by digest");
        match client.block(Query::Digest(digest)).await {
            Ok(Payload::Block(block)) => {
                let value = Bytes::from(block.encode().to_vec());
                Self::deliver(key, value, handler, subscribers).await
            }
            Ok(_) => {
                warn!(?digest, "wrong payload returned for block by digest");
                true
            }
            Err(error) => {
                warn!(?digest, ?error, "failed to fetch block by digest");
                true
            }
        }
    }

    async fn fetch_finalized_by_height(
        key: Key,
        height: Height,
        client: C,
        handler: handler::Handler<Digest>,
        subscribers: Subscribers,
    ) -> bool {
        debug!(height = height.get(), "fetching finalized block by height");
        match client.block(Query::Index(height.get())).await {
            Ok(Payload::Finalized(finalized)) => {
                let value = Bytes::from(
                    (finalized.proof.clone(), finalized.block.clone())
                        .encode()
                        .to_vec(),
                );
                Self::deliver(key, value, handler, subscribers).await
            }
            Ok(_) => {
                warn!(
                    height = height.get(),
                    "wrong payload returned for finalized block by height"
                );
                true
            }
            Err(error) => {
                warn!(
                    height = height.get(),
                    ?error,
                    "failed to fetch finalized block by height"
                );
                true
            }
        }
    }

    async fn fetch_notarized_by_round(
        key: Key,
        round: Round,
        client: C,
        handler: handler::Handler<Digest>,
        subscribers: Subscribers,
    ) -> bool {
        let view = round.view().get();
        debug!(view, "fetching notarized block by round");
        match client.notarized(IndexQuery::Index(view)).await {
            Ok(notarized) => {
                let value = Bytes::from(
                    (notarized.proof.clone(), notarized.block.clone())
                        .encode()
                        .to_vec(),
                );
                Self::deliver(key, value, handler, subscribers).await
            }
            Err(error) => {
                warn!(view, ?error, "failed to fetch notarized block by round");
                true
            }
        }
    }

    async fn deliver(
        key: Key,
        value: Bytes,
        mut handler: handler::Handler<Digest>,
        subscribers: Subscribers,
    ) -> bool {
        let subscribers = subscribers.lock().clone();
        let Ok(subscribers) = NonEmptyVec::try_from(subscribers) else {
            return false;
        };
        let response = handler.deliver(Delivery { key, subscribers }, value);
        match response.await {
            Ok(true) => false,
            Ok(false) => {
                warn!(?key, "marshal rejected source resolver delivery");
                true
            }
            Err(error) => {
                warn!(
                    ?key,
                    ?error,
                    "marshal dropped source resolver delivery response"
                );
                true
            }
        }
    }
}
