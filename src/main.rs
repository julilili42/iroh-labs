use crate::{
    cli::{Command, confirm, parse_arguments, select_receiver},
    receiver::{OfferDecision, OfferProtocol, OfferRequest},
    sender::run_sender,
};
use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, endpoint::presets, endpoint_info::UserData, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use iroh_tickets::endpoint::EndpointTicket;
use tokio::sync::{mpsc, watch};
mod cli;
mod mdns;
mod protocol;
mod receiver;
mod sender;
mod ui;

#[derive(Debug)]
pub struct Runtime {
    endpoint: Endpoint,
    store: MemStore,
    router: Router,
    ticket: EndpointTicket,
    offer_rx: mpsc::Receiver<OfferRequest>,
    peer_rx: watch::Receiver<Vec<(UserData, EndpointAddr)>>,
    progress_rx: watch::Receiver<u64>,
    progress_tx: watch::Sender<u64>,
}

pub async fn start_iroh() -> Result<Runtime> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::bind(presets::N0).await?;
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();

    let ticket = EndpointTicket::new(endpoint.addr());
    println!("Ticket:");
    println!("{ticket}");

    // ui receives offers and can decide
    let (offer_tx, offer_rx) = mpsc::channel(10);
    // download progress
    let (progress_tx, progress_rx) = watch::channel(0_u64);
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs_handler = BlobsProtocol::new(&store, None);
    let offer_handler = OfferProtocol::new(&endpoint, &store, offer_tx, progress_tx.clone());

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

    Ok(Runtime {
        endpoint,
        store,
        router,
        ticket,
        offer_rx,
        peer_rx,
        progress_rx,
        progress_tx,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    // Grab all passed in arguments, the first one is the binary itself, so we skip it.
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Convert to &str, so we can pattern-match easily:
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    let command = parse_arguments(arg_refs)?;

    if let Command::Version | Command::Help = command {
        return Ok(());
    };

    let mut runtime = start_iroh().await?;
    let router = runtime.router.clone();
    let result = async {
        match command {
            Command::Ui => ui::run(runtime),
            Command::Send {
                filename,
                endpoint_addr,
            } => {
                let endpoint_addr = match endpoint_addr {
                    Some(addr) => addr,
                    None => select_receiver(runtime.peer_rx.clone()).await?,
                };

                run_sender(
                    runtime.progress_tx,
                    &filename,
                    &runtime.endpoint,
                    &runtime.store,
                    endpoint_addr,
                )
                .await
                .map(|_| ())
            }
            Command::Receive { download_dir } => loop {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        return result.map_err(Into::into);
                    }
                    request = runtime.offer_rx.recv() => {
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
            },
            _ => unreachable!(),
        }
    }
    .await;

    let shutdown_result = router.shutdown().await;
    result?;
    shutdown_result?;

    Ok(())
}
