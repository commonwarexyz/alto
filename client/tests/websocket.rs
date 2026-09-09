use alto_client::{ClientBuilder, Error};
use alto_types::{Identity, StandardScheme, NAMESPACE};
use commonware_math::algebra::CryptoGroup;
use commonware_parallel::Sequential;
use futures::StreamExt;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tokio_tungstenite::tungstenite::{error::CapacityError, Error as WsError};

/// Send raw frames through a real client and return its transport or artifact error.
async fn receive_error(block_size: Option<u32>, frames: Vec<u8>) -> Error {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
        socket.get_mut().write_all(&frames).await.unwrap();
        socket.get_mut().shutdown().await.unwrap();
    });
    let mut builder = ClientBuilder::new(
        &format!("http://{addr}"),
        StandardScheme::certificate_verifier(NAMESPACE, Identity::generator()),
        Sequential,
    );
    if let Some(block_size) = block_size {
        builder = builder.with_block_size(block_size);
    }
    let client = builder.build();
    let mut stream = client.listen().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("frame processing stalled")
        .expect("frame did not produce a result");
    server.await.unwrap();
    match result {
        Err(error) => error,
        Ok(_) => panic!("unexpected consensus artifact"),
    }
}

#[tokio::test]
async fn oversized_frames_are_rejected_from_the_header() {
    for (block_size, limit) in [
        (None, 5 * 1024 * 1024 + 1),
        (Some(0), 1024 * 1024 + 1),
        (Some(4096), 1024 * 1024 + 4097),
    ] {
        for fragmented in [false, true] {
            // Announce one byte beyond the receive limit, without sending its payload.
            let mut frames = Vec::new();
            if fragmented {
                frames.extend_from_slice(&[0x02, 0x01, 0x00]);
            }
            frames.extend_from_slice(&[if fragmented { 0x80 } else { 0x82 }, 0x7f]);
            frames.extend_from_slice(&((limit + 1) as u64).to_be_bytes());
            assert!(matches!(
                receive_error(block_size, frames).await,
                Error::Tungstenite(WsError::Capacity(CapacityError::MessageTooLong {
                    size,
                    max_size,
                })) if size == limit + 1 && max_size == limit
            ));
        }
    }
}

#[tokio::test]
async fn streaming_budget_includes_the_message_kind() {
    // An HTTP artifact can fill the entire encoding allowance. Its stream adds one kind byte.
    let length = 1024 * 1024 + 1;
    let mut frame = vec![0x82, 0x7f];
    frame.extend_from_slice(&(length as u64).to_be_bytes());
    frame.resize(frame.len() + length, 0xff);

    // Reaching kind dispatch proves that the complete message passed the transport limit.
    assert!(matches!(
        receive_error(Some(0), frame).await,
        Error::UnexpectedResponse
    ));
}

#[tokio::test]
async fn fragmented_messages_share_the_receive_budget() {
    // Each frame fits by itself, but their combined payload exceeds the message budget.
    let length = 1024 * 1024;
    let mut frames = vec![0x02, 0x7f];
    frames.extend_from_slice(&(length as u64).to_be_bytes());
    frames.resize(frames.len() + length, 0xff);
    frames.extend_from_slice(&[0x80, 0x02, 0xff, 0xff]);
    assert!(matches!(
        receive_error(Some(0), frames).await,
        Error::Tungstenite(WsError::Capacity(CapacityError::MessageTooLong {
            size,
            max_size,
        })) if size == length + 2 && max_size == length + 1
    ));
}
