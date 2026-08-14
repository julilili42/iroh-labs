use std::path::Path;

use anyhow::Result;
use iroh::{
    Endpoint,
    endpoint::{SendStream, presets},
    protocol::Router,
};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore, ticket::BlobTicket};
use n0_error::StackResultExt;
use tokio::io::AsyncWriteExt;

use crate::{
    mdns,
    protocol::{self, Offer, OfferProtocol, download_finished, send_transfer_offer},
};

pub async fn start_iroh() -> Result<(Endpoint, MemStore, Router)> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs_handler = BlobsProtocol::new(&store, None);
    let offer_handler = OfferProtocol::new(&endpoint, &store);

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs_handler)
        .accept(protocol::ALPN, offer_handler)
        .spawn();

    Ok((endpoint, store, router))
}

pub async fn run_sender(
    filename: String,
    endpoint: Endpoint,
    router: Router,
    store: &MemStore,
) -> Result<()> {
    let mdns = mdns::enable(&endpoint)?;
    let receiver_addr = mdns::discover_one(&mdns).await?;

    let abs_path = std::path::absolute(Path::new(&filename))?;
    let filesize = tokio::fs::metadata(&abs_path).await?.len();

    println!("Hashing file.");

    // When we import a blob, we get back a "tag" that refers to said blob in the store
    // and allows us to control when/if it gets garbage-collected
    let tag = store.blobs().add_path(abs_path).await?;
    let ticket = BlobTicket::new(endpoint.id().into(), tag.hash, tag.format);

    println!("File hashed.");

    let offer = Offer::new(&filename, filesize, &ticket);

    let conn = endpoint.connect(receiver_addr, protocol::ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    let accepted = send_transfer_offer(&mut send, &mut recv, &offer).await?;
    match accepted {
        true => {
            println!("Accepted offer");
            download_finished(&mut recv).await?;
            println!("Download finished");
        }
        false => println!("Declined offer"),
    }
    // Gracefully shut down the endpoint
    println!("Shutting down.");
    router.shutdown().await?;

    Ok(())
}

pub async fn download(
    endpoint: &Endpoint,
    store: &MemStore,
    offer: Offer,
    send: &mut SendStream,
) -> Result<()> {
    let abs_path = std::path::absolute(offer.filename)?;
    let downloader = store.downloader(endpoint);

    println!("Starting download.");

    downloader
        .download(offer.ticket.hash(), Some(offer.ticket.addr().id))
        .await
        .context("failed to download")?;

    println!("Finished download.");

    println!("Copying to destination.");

    store
        .blobs()
        .export(offer.ticket.hash(), abs_path)
        .await
        .context("failed to export")?;

    println!("Finished copying.");

    send.write_u8(1)
        .await
        .context("failed to send download byte")?;
    send.finish()?;

    // Gracefully shut down the endpoint
    println!("Shutting down.");
    Ok(())
}

pub async fn run_receiver(endpoint: Endpoint, router: Router) -> Result<()> {
    // We initialize an in-memory backing store for iroh-blobs
    let _mdns = mdns::enable(&endpoint)?;

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;
    Ok(())
}
