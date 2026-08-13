use std::path::PathBuf;

use anyhow::Result;
use iroh::protocol::Router;
use iroh_blobs::{BlobsProtocol, store::mem::MemStore, ticket::BlobTicket};

use iroh::{Endpoint, endpoint::presets};

use crate::{
    mdns,
    protocol::{Offer, accept_transfer_offer, send_transfer_offer},
};

pub async fn run_sender(filename: &str) -> Result<()> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;

    let mdns = mdns::enable(&endpoint)?;
    let receiver_addr = mdns::discover_one(&mdns).await?;

    let filename_str = filename.to_string();
    let filename: PathBuf = filename.parse()?;

    let abs_path = std::path::absolute(&filename)?;

    let filesize = tokio::fs::metadata(&abs_path).await?.len();

    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs = BlobsProtocol::new(&store, None);

    println!("Hashing file.");

    // When we import a blob, we get back a "tag" that refers to said blob in the store
    // and allows us to control when/if it gets garbage-collected
    let tag = store.blobs().add_path(abs_path).await?;

    let endpoint_id = endpoint.id();
    let ticket = BlobTicket::new(endpoint_id.into(), tag.hash, tag.format);

    // println!("File hashed. Fetch this file by running:");
    // println!("cargo run -- receive {ticket} {}", filename.display());

    let offer = Offer {
        filename: filename_str,
        filesize,
        ticket,
    };

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    let accepted = send_transfer_offer(router.endpoint(), receiver_addr, &offer).await?;

    match accepted {
        true => println!("Accepted offer"),
        false => println!("Declined offer"),
    }
    // Gracefully shut down the endpoint
    println!("Shutting down.");
    router.shutdown().await?;

    Ok(())
}

pub async fn download(endpoint: &Endpoint, store: &MemStore, offer: Offer) -> Result<()> {
    let abs_path = std::path::absolute(offer.filename)?;
    let downloader = store.downloader(&endpoint);

    println!("Starting download.");

    downloader
        .download(offer.ticket.hash(), Some(offer.ticket.addr().id))
        .await?;

    println!("Finished download.");

    println!("Copying to destination.");

    store.blobs().export(offer.ticket.hash(), abs_path).await?;

    println!("Finished copying.");

    // Gracefully shut down the endpoint
    println!("Shutting down.");

    Ok(())
}

pub async fn run_receiver() -> Result<()> {
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    let endpoint = Endpoint::bind(presets::N0).await?;

    let _mdns = mdns::enable(&endpoint)?;

    let router = accept_transfer_offer(endpoint.clone(), store).await?;

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;

    Ok(())
}
