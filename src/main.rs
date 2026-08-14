use std::{
    env,
    path::{self, PathBuf},
};

use anyhow::Result;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};

use crate::{
    receiver::{OfferProtocol, run_receiver},
    sender::run_sender,
};

mod mdns;
mod protocol;
mod receiver;
mod sender;

pub async fn start_iroh(download_dir: PathBuf) -> Result<(Endpoint, MemStore, Router)> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs_handler = BlobsProtocol::new(&store, None);
    let offer_handler = OfferProtocol::new(&endpoint, &store, &download_dir);

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs_handler)
        .accept(protocol::ALPN, offer_handler)
        .spawn();

    Ok((endpoint, store, router))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    // Grab all passed in arguments, the first one is the binary itself, so we skip it.
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Convert to &str, so we can pattern-match easily:
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    let download_dir = match arg_refs.as_slice() {
        ["receive", dir] => path::absolute(dir)?,
        _ => env::current_dir()?,
    };

    let (endpoint, store, router) = start_iroh(download_dir).await?;

    match arg_refs.as_slice() {
        ["send", filename] => run_sender(filename.to_string(), endpoint, router, &store).await?,
        ["receive", _] => run_receiver(endpoint, router).await?,
        _ => {
            println!("Usage:");
            println!("    cargo run -- send <FILE>");
            println!("    cargo run -- receive <DOWNLOAD_DIR>");
        }
    }

    Ok(())
}
