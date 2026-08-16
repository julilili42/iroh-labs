use std::{
    env,
    io::{self, stdin, stdout},
    path::{self, PathBuf},
    time::Duration,
};

use crate::{
    receiver::{OfferProtocol, run_receiver},
    sender::run_sender,
};
use anyhow::Result;
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use iroh_mdns_address_lookup::MdnsAddressLookup;
use tokio::{sync::watch, time};
mod mdns;
mod protocol;
mod receiver;
mod sender;

pub async fn start_iroh(
    download_dir: PathBuf,
) -> Result<(Endpoint, MemStore, Router, MdnsAddressLookup)> {
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

    let device_name = whoami::devicename().or_else(|_| whoami::hostname())?;
    let mdns = mdns::enable(&endpoint, &device_name)?;

    Ok((endpoint, store, router, mdns))
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

    let (endpoint, store, router, mdns) = start_iroh(download_dir).await?;

    match arg_refs.as_slice() {
        ["send", filename] => {
            let (tx, mut rx) = watch::channel(Vec::new());
            tokio::spawn(mdns::discover(mdns, tx));

            let search_str = "Searching in local net...";
            println!("{}", search_str);

            while rx.changed().await.is_ok() && rx.borrow().is_empty() {}
            time::sleep(Duration::from_secs(2)).await;

            let devices = rx.borrow().clone();

            let lines = devices
                .iter()
                .enumerate()
                .map(|(i, (user_data, _))| format!("{}. {}", i + 1, user_data));

            let title_str = "Receiver list.";
            let max_len = lines
                .clone()
                .map(|line| line.len())
                .max()
                .unwrap_or(0)
                .max(title_str.len())
                .max(search_str.len());

            println!("\n{}", title_str);
            println!("{}", "-".repeat(max_len));
            for line in lines {
                println!("{}", line);
            }
            println!("{}", "-".repeat(max_len));
            println!("\nSelect receiver:");
            let mut input = String::new();
            io::Write::flush(&mut stdout()).unwrap();
            stdin().read_line(&mut input).unwrap();

            if let Ok(idx) = input.trim().parse::<usize>()
                && idx > 0
                && idx <= devices.len()
            {
                let (_, endpoint_addr) = &devices[idx - 1];
                run_sender(
                    filename.to_string(),
                    endpoint,
                    router,
                    &store,
                    endpoint_addr.clone(),
                )
                .await?
            } else {
                router.shutdown().await?;
            }
        }
        ["receive"] | ["receive", _] => run_receiver(router).await?,
        _ => {
            println!("Usage:");
            println!("    cargo run -- send <FILE>");
            println!("    cargo run -- receive [DOWNLOAD_DIR]");
            router.shutdown().await?;
        }
    }

    Ok(())
}
