use std::{
    env,
    path::{self, PathBuf},
};

use crate::{
    cli::{confirm, print_usage, select_receiver},
    receiver::{OfferProtocol, OfferRequest},
    sender::run_sender,
};
use anyhow::Result;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use iroh_mdns_address_lookup::MdnsAddressLookup;
use tokio::sync::{mpsc, watch};
mod cli;
mod mdns;
mod protocol;
mod receiver;
mod sender;

pub async fn start_iroh(
    download_dir: PathBuf,
) -> Result<(
    Endpoint,
    MemStore,
    Router,
    MdnsAddressLookup,
    mpsc::Receiver<OfferRequest>,
)> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();

    // ui receives offers and can decide
    let (offer_tx, offer_rx) = mpsc::channel(10);
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs_handler = BlobsProtocol::new(&store, None);
    let offer_handler = OfferProtocol::new(&endpoint, &store, &download_dir, offer_tx);

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs_handler)
        .accept(protocol::ALPN, offer_handler)
        .spawn();

    let device_name = whoami::devicename().or_else(|_| whoami::hostname())?;
    let mdns = mdns::enable(&endpoint, &device_name)?;

    Ok((endpoint, store, router, mdns, offer_rx))
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

    let (endpoint, store, router, mdns, mut offer_rx) = start_iroh(download_dir).await?;

    let result = async {
        match arg_refs.as_slice() {
            ["send", filename] => {
                let (discover_tx, discover_rx) = watch::channel(Vec::new());
                tokio::spawn(mdns::discover(mdns, discover_tx));

                drop(offer_rx);

                let endpoint_addr = select_receiver(discover_rx).await?;
                run_sender(filename.to_string(), endpoint, &store, endpoint_addr).await
            }
            ["receive"] | ["receive", _] => loop {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        return result.map_err(Into::into);
                    }
                    request = offer_rx.recv() => {
                        let Some((offer, tx)) = request else {
                            break Ok(());
                        };
                        let decision = confirm(&offer).await?;
                        let _ = tx.send(decision);
                    }
                }
            },
            _ => {
                print_usage();
                Ok(())
            }
        }
    }
    .await;

    let shutdown_result = router.shutdown().await;
    result?;
    shutdown_result?;

    Ok(())
}
