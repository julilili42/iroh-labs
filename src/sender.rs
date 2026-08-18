use std::path::Path;

use crate::protocol::{
    self, DecisionStatus, DownloadStatus, Offer, download_finished, transfer_decision,
};
use anyhow::Result;
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket};
use n0_error::StackResultExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    Completed,
    Declined,
}

pub async fn run_sender(
    filename: String,
    endpoint: Endpoint,
    store: &MemStore,
    endpoint_addr: EndpointAddr,
) -> Result<SendOutcome> {
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
            return Ok(SendOutcome::Declined);
        }
    }

    match download_finished(&mut recv).await? {
        DownloadStatus::Completed => {
            println!("Download finished.");
            Ok(SendOutcome::Completed)
        }
        DownloadStatus::Failed => {
            println!("Download failed.");
            anyhow::bail!("Failed to download.")
        }
    }
}
