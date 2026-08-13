use std::path::PathBuf;

use anyhow::Result;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore, ticket::BlobTicket};

pub async fn run_sender(filename: &str) -> Result<()> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;

    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs = BlobsProtocol::new(&store, None);

    let filename: PathBuf = filename.parse()?;
    let abs_path = std::path::absolute(&filename)?;

    println!("Hashing file.");

    // When we import a blob, we get back a "tag" that refers to said blob in the store
    // and allows us to control when/if it gets garbage-collected
    let tag = store.blobs().add_path(abs_path).await?;

    let endpoint_id = endpoint.id();
    let ticket = BlobTicket::new(endpoint_id.into(), tag.hash, tag.format);

    println!("File hashed. Fetch this file by running:");
    println!("cargo run -- receive {ticket} {}", filename.display());

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    tokio::signal::ctrl_c().await?;

    // Gracefully shut down the endpoint
    println!("Shutting down.");
    router.shutdown().await?;

    Ok(())
}

pub async fn run_receiver(ticket: &str, filename: &str) -> Result<()> {
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    let endpoint = Endpoint::bind(presets::N0).await?;

    let filename: PathBuf = filename.parse()?;
    let ticket: BlobTicket = ticket.parse()?;
    let abs_path = std::path::absolute(filename)?;
    // For receiving files, we create a "downloader" that allows us to fetch files
    // from other endpoints via iroh connections
    let downloader = store.downloader(&endpoint);

    println!("Starting download.");

    downloader
        .download(ticket.hash(), Some(ticket.addr().id))
        .await?;

    println!("Finished download.");

    println!("Copying to destination.");

    store.blobs().export(ticket.hash(), abs_path).await?;

    println!("Finished copying.");

    // Gracefully shut down the endpoint
    println!("Shutting down.");
    endpoint.close().await;

    Ok(())
}
