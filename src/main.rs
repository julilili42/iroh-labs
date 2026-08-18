use std::{env, path};

use crate::{
    cli::{confirm, print_usage, select_receiver},
    receiver::{OfferDecision, OfferProtocol, OfferRequest},
    sender::run_sender,
};
use anyhow::{Result, anyhow};
use iroh::{Endpoint, EndpointAddr, endpoint::presets, endpoint_info::UserData, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use tokio::sync::{mpsc, watch};
mod cli;
mod mdns;
mod protocol;
mod receiver;
mod sender;
mod ui;

pub async fn start_iroh() -> Result<(
    Endpoint,
    MemStore,
    Router,
    EndpointTicket,
    mpsc::Receiver<OfferRequest>,
    watch::Receiver<Vec<(UserData, EndpointAddr)>>,
)> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();

    let ticket = EndpointTicket::new(endpoint.addr());
    println!("{ticket}");

    // ui receives offers and can decide
    let (offer_tx, offer_rx) = mpsc::channel(10);
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs_handler = BlobsProtocol::new(&store, None);
    let offer_handler = OfferProtocol::new(&endpoint, &store, offer_tx);

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs_handler)
        .accept(protocol::ALPN, offer_handler)
        .spawn();

    let device_name = whoami::devicename().or_else(|_| whoami::hostname())?;

    let mdns = mdns::enable(&endpoint, &device_name)?;
    let (peer_tx, peer_rx) = watch::channel(Vec::new());
    tokio::spawn(mdns::discover(mdns, peer_tx));

    Ok((endpoint, store, router, ticket, offer_rx, peer_rx))
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

    let (endpoint, store, router, ticket, mut offer_rx, peer_rx) = start_iroh().await?;

    let result = async {
        match arg_refs.as_slice() {
            [] => ui::run(peer_rx, offer_rx, endpoint, store, ticket),
            ["send", filename, ticket_str] => {
                drop(offer_rx);

                let ticket = EndpointTicket::decode_string(&ticket_str)
                    .map_err(|e| anyhow!("failed to parse ticket: {}", e))?;

                run_sender(
                    filename.to_string(),
                    endpoint,
                    &store,
                    ticket.endpoint_addr().clone(),
                )
                .await
                .map(|_| ())
            }
            ["send", filename] => {
                drop(offer_rx);

                let endpoint_addr = select_receiver(peer_rx).await?;
                run_sender(filename.to_string(), endpoint, &store, endpoint_addr)
                    .await
                    .map(|_| ())
            }
            ["receive"] | ["receive", _] => {
                let _peer = peer_rx;
                loop {
                    tokio::select! {
                        result = tokio::signal::ctrl_c() => {
                            return result.map_err(Into::into);
                        }
                        request = offer_rx.recv() => {
                            let Some((offer, tx)) = request else {
                                break Ok(());
                            };
                            let decision = if confirm(&offer).await? {
                                OfferDecision::Accept(download_dir.clone())
                            } else {
                                OfferDecision::Decline
                            };
                            let _ = tx.send(decision);
                        }
                    }
                }
            }
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
