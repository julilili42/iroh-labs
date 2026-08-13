use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, endpoint::Connection, protocol::AcceptError};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};

const ALPN: &[u8] = b"iroh-example/echo/0";

#[derive(Debug, Clone)]
struct Echo;

impl iroh::protocol::ProtocolHandler for Echo {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // We can get the remote's endpoint id from the connection.
        let endpoint_id = connection.remote_id();
        println!("accepted connection from {endpoint_id}");

        // Our protocol is a simple request-response protocol, so we expect the
        // connecting peer to open a single bi-directional stream.
        let (mut send, mut recv) = connection.accept_bi().await?;

        // Echo any bytes received back directly.
        // This will keep copying until the sender signals the end of data on the stream.
        let bytes_sent = tokio::io::copy(&mut recv, &mut send).await?;
        println!("Copied over {bytes_sent} byte(s)");

        // By calling `finish` on the send stream we signal that we will not send anything
        // further, which makes the receive stream on the other end terminate.
        send.finish()?;

        // Wait until the remote closes the connection, which it does once it
        // received the response.
        connection.closed().await;

        Ok(())
    }
}

async fn start_accept_side() -> anyhow::Result<iroh::protocol::Router> {
    let endpoint = iroh::Endpoint::bind(iroh::endpoint::presets::N0).await?;

    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs = BlobsProtocol::new(&store, None);

    let router = iroh::protocol::Router::builder(endpoint)
        .accept(ALPN, Echo) // This makes the router handle incoming connections with our ALPN via Echo::accept!
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    Ok(router)
}

async fn connect_side(addr: EndpointAddr, data: &[u8]) -> Result<()> {
    let endpoint = Endpoint::bind(iroh::endpoint::presets::N0).await?;

    // Open a connection to the accepting endpoint
    let conn = endpoint.connect(addr, ALPN).await?;

    // Open a bidirectional QUIC stream
    let (mut send, mut recv) = conn.open_bi().await?;

    // Send some data to be echoed
    send.write_all(data).await?;

    // Signal the end of data for this particular stream
    send.finish()?;

    // Receive the echo, but limit reading up to maximum 1000 bytes
    let response = recv.read_to_end(1000).await?;
    assert_eq!(&response, b"Hello, world!");

    // Explicitly close the whole connection.
    conn.close(0u32.into(), b"bye!");

    // The above call only queues a close message to be sent (see how it's not async!).
    // We need to actually call this to make sure this message is sent out.
    endpoint.close().await;
    // If we don't call this, but continue using the endpoint, we then the queued
    // close call will eventually be picked up and sent.
    // But always try to wait for endpoint.close().await to go through before dropping
    // the endpoint to ensure any queued messages are sent through and connections are
    // closed gracefully.
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let router = start_accept_side().await?;
    let endpoint_addr = router.endpoint().addr();

    let data = b"Hello, world!";
    connect_side(endpoint_addr, data).await?;

    // This makes sure the endpoint in the router is closed properly and connections close gracefully
    router.shutdown().await?;

    Ok(())
}
