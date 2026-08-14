use std::path::Path;

use anyhow::Result;
use iroh::{Endpoint, protocol::Router};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket};
use n0_error::StackResultExt;

use crate::{
    mdns,
    protocol::{self, Offer, download_finished, send_transfer_offer},
};

pub async fn run_sender(
    filename: String,
    endpoint: Endpoint,
    router: Router,
    store: &MemStore,
) -> Result<()> {
    let mdns = mdns::enable(&endpoint)?;
    let receiver_addr = mdns::discover_one(&mdns).await?;

    let file_path = Path::new(&filename);
    let safe_name = file_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("invalid filename")?;

    let abs_path = std::path::absolute(file_path)?;
    let filesize = tokio::fs::metadata(&abs_path).await?.len();

    println!("Hashing file.");

    // When we import a blob, we get back a "tag" that refers to said blob in the store
    // and allows us to control when/if it gets garbage-collected
    let tag = store.blobs().add_path(abs_path).await?;
    let ticket = BlobTicket::new(endpoint.id().into(), tag.hash, tag.format);

    println!("File hashed.");

    let offer = Offer::new(safe_name, filesize, &ticket);

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
