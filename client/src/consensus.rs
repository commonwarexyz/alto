use crate::{Client, Error, IndexQuery, Query};
use alto_types::{Block, Finalized, Kind, Notarized, Seed};
use bytes::Bytes;
use futures::{channel::mpsc::unbounded, Stream, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message as TMessage};

fn seed_upload_path(base: String) -> String {
    format!("{}/seed", base)
}

fn seed_get_path(base: String, query: &IndexQuery) -> String {
    format!("{}/seed/{}", base, query.serialize())
}

fn notarization_upload_path(base: String) -> String {
    format!("{}/notarization", base)
}

fn notarization_get_path(base: String, query: &IndexQuery) -> String {
    format!("{}/notarization/{}", base, query.serialize())
}

fn finalization_upload_path(base: String) -> String {
    format!("{}/finalization", base)
}

fn finalization_get_path(base: String, query: &IndexQuery) -> String {
    format!("{}/finalization/{}", base, query.serialize())
}

/// There is no block upload path. Blocks are uploaded as a byproduct of notarization
/// and finalization uploads.
fn block_get_path(base: String, query: &Query) -> String {
    format!("{}/block/{}", base, query.serialize())
}

fn listen_path(base: String) -> String {
    format!("{}/consensus/ws", base)
}

pub enum Payload {
    Finalized(Box<Finalized>),
    Block(Block),
}

pub enum Message {
    Seed(Seed),
    Notarization(Notarized),
    Finalization(Finalized),
}

impl Client {
    pub async fn seed_upload(&self, seed: Bytes) -> Result<(), Error> {
        let result = self
            .client
            .post(seed_upload_path(self.uri.clone()))
            .body(seed)
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        Ok(())
    }

    pub async fn seed_get(&self, query: IndexQuery) -> Result<Seed, Error> {
        // Get the seed
        let result = self
            .client
            .get(seed_get_path(self.uri.clone(), &query))
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        let bytes = result.bytes().await.map_err(Error::Reqwest)?;
        let result = Seed::deserialize(Some(&self.public), &bytes).ok_or(Error::InvalidData)?;

        // Verify the seed matches the query
        match query {
            IndexQuery::Latest => {}
            IndexQuery::Index(index) => {
                if result.view != index {
                    return Err(Error::InvalidData);
                }
            }
        }
        Ok(result)
    }

    pub async fn notarization_upload(&self, notarized: Bytes) -> Result<(), Error> {
        let result = self
            .client
            .post(notarization_upload_path(self.uri.clone()))
            .body(notarized)
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        Ok(())
    }

    pub async fn notarization_get(&self, query: IndexQuery) -> Result<Notarized, Error> {
        // Get the notarization
        let result = self
            .client
            .get(notarization_get_path(self.uri.clone(), &query))
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        let bytes = result.bytes().await.map_err(Error::Reqwest)?;
        let result =
            Notarized::deserialize(Some(&self.public), &bytes).ok_or(Error::InvalidData)?;

        // Verify the notarization matches the query
        match query {
            IndexQuery::Latest => {}
            IndexQuery::Index(index) => {
                if result.proof.view != index {
                    return Err(Error::InvalidData);
                }
            }
        }
        Ok(result)
    }

    pub async fn finalization_upload(&self, finalized: Bytes) -> Result<(), Error> {
        let result = self
            .client
            .post(finalization_upload_path(self.uri.clone()))
            .body(finalized)
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        Ok(())
    }

    pub async fn finalization_get(&self, query: IndexQuery) -> Result<Finalized, Error> {
        // Get the finalization
        let result = self
            .client
            .get(finalization_get_path(self.uri.clone(), &query))
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        let bytes = result.bytes().await.map_err(Error::Reqwest)?;
        let result =
            Finalized::deserialize(Some(&self.public), &bytes).ok_or(Error::InvalidData)?;

        // Verify the finalization matches the query
        match query {
            IndexQuery::Latest => {}
            IndexQuery::Index(index) => {
                if result.proof.view != index {
                    return Err(Error::InvalidData);
                }
            }
        }
        Ok(result)
    }

    pub async fn block_get(&self, query: Query) -> Result<Payload, Error> {
        // Get the block
        let result = self
            .client
            .get(block_get_path(self.uri.clone(), &query))
            .send()
            .await
            .map_err(Error::Reqwest)?;
        if !result.status().is_success() {
            return Err(Error::Failed(result.status()));
        }
        let bytes = result.bytes().await.map_err(Error::Reqwest)?;

        // Verify the block matches the query
        let result = match query {
            Query::Latest => {
                let result =
                    Finalized::deserialize(Some(&self.public), &bytes).ok_or(Error::InvalidData)?;
                Payload::Finalized(Box::new(result))
            }
            Query::Index(index) => {
                let result =
                    Finalized::deserialize(Some(&self.public), &bytes).ok_or(Error::InvalidData)?;
                if result.block.height != index {
                    return Err(Error::InvalidData);
                }
                Payload::Finalized(Box::new(result))
            }
            Query::Digest(digest) => {
                let result = Block::deserialize(&bytes).ok_or(Error::InvalidData)?;
                if result.digest() != digest {
                    return Err(Error::InvalidData);
                }
                Payload::Block(result)
            }
        };
        Ok(result)
    }

    pub async fn listen(&self) -> Result<impl Stream<Item = Result<Message, Error>>, Error> {
        // Connect to the websocket endpoint
        let (stream, _) = connect_async(listen_path(self.ws_uri.clone()))
            .await
            .map_err(Error::from)?;
        let (_, read) = stream.split();

        // Create an unbounded channel for streaming consensus messages
        let public = self.public.clone();
        let (sender, receiver) = unbounded();
        tokio::spawn(async move {
            read.for_each(|message| async {
                match message {
                    Ok(TMessage::Binary(data)) => {
                        // Get kind
                        let kind = data[0];
                        let Some(kind) = Kind::from_u8(kind) else {
                            let _ = sender.unbounded_send(Err(Error::InvalidData));
                            return;
                        };
                        let data = &data[1..];

                        // Deserialize the message
                        match kind {
                            Kind::Seed => {
                                if let Some(seed) = Seed::deserialize(Some(&public), data) {
                                    let _ = sender.unbounded_send(Ok(Message::Seed(seed)));
                                } else {
                                    let _ = sender.unbounded_send(Err(Error::InvalidData));
                                }
                            }
                            Kind::Notarization => {
                                if let Some(payload) = Notarized::deserialize(Some(&public), data) {
                                    let _ =
                                        sender.unbounded_send(Ok(Message::Notarization(payload)));
                                } else {
                                    let _ = sender.unbounded_send(Err(Error::InvalidData));
                                }
                            }
                            Kind::Nullification => {} // Ignore nullifications
                            Kind::Finalization => {
                                if let Some(payload) = Finalized::deserialize(Some(&public), data) {
                                    let _ =
                                        sender.unbounded_send(Ok(Message::Finalization(payload)));
                                } else {
                                    let _ = sender.unbounded_send(Err(Error::InvalidData));
                                }
                            }
                        }
                    }
                    Ok(_) => {} // Ignore non-binary messages.
                    Err(e) => {
                        let _ = sender.unbounded_send(Err(Error::from(e)));
                    }
                }
            })
            .await;
        });
        Ok(receiver)
    }
}
