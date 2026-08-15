use std::path::Path;

use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, protocol::Router};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket};
use n0_error::StackResultExt;

use crate::{
    protocol::{self, Offer, download_finished, transfer_decision},
    protocol::{DecisionStatus, DownloadStatus},
};

pub async fn run_sender(
    filename: String,
    endpoint: Endpoint,
    router: Router,
    store: &MemStore,
    endpoint_addr: EndpointAddr,
) -> Result<()> {
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

    let conn = endpoint.connect(endpoint_addr, protocol::ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    offer.write_to(&mut send).await?;

    match transfer_decision(&mut recv).await? {
        DecisionStatus::Accepted => println!("Accepted offer."),
        DecisionStatus::Declined => {
            println!("Declined offer.");
            println!("Shutting down.");
            router.shutdown().await?;
            return Ok(());
        }
    }

    match download_finished(&mut recv).await? {
        DownloadStatus::Completed => {
            println!("Download finished.");
            println!("Shutting down.");
            router.shutdown().await?;
            Ok(())
        }
        DownloadStatus::Failed => {
            println!("Download failed.");
            println!("Shutting down.");
            router.shutdown().await?;
            anyhow::bail!("Failed to download.")
        }
    }
}
