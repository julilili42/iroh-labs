use std::path::{Path, PathBuf};

use anyhow::Result;
use iroh::{
    Endpoint,
    endpoint::{Connection, SendStream},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_blobs::{api::downloader::DownloadProgressItem, store::mem::MemStore};
use n0_error::{StackResultExt, e};
use n0_future::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{mdns, protocol::Offer};

pub async fn run_receiver(endpoint: Endpoint, router: Router) -> Result<()> {
    let _mdns = mdns::enable(&endpoint)?;

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct OfferProtocol {
    pub endpoint: Endpoint,
    pub store: MemStore,
    pub download_dir: PathBuf,
}

impl OfferProtocol {
    pub fn new(endpoint: &Endpoint, store: &MemStore, download_dir: &Path) -> Self {
        Self {
            endpoint: endpoint.clone(),
            store: store.clone(),
            download_dir: download_dir.to_path_buf(),
        }
    }
}

impl ProtocolHandler for OfferProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let offer = Offer::read_from(&mut recv).await.map_err(accept_error)?;

        if offer.ticket.addr().id != connection.remote_id() {
            return Err(e!(AcceptError::NotAllowed));
        }

        let accepted = confirm(&offer, &mut send)
            .await
            .map_err(AcceptError::from_err)?;

        if accepted {
            download(
                &self.endpoint,
                &self.store,
                &self.download_dir,
                offer,
                &mut send,
            )
            .await
            .map_err(accept_error)?;
        } else {
            send.finish()?;
        }

        connection.closed().await;
        Ok(())
    }
}

fn accept_error(error: anyhow::Error) -> AcceptError {
    AcceptError::from_boxed(error.into_boxed_dyn_error())
}

pub async fn download(
    endpoint: &Endpoint,
    store: &MemStore,
    download_dir: &Path,
    offer: Offer,
    send: &mut SendStream,
) -> Result<()> {
    let filename = Path::new(&offer.filename)
        .file_name()
        .context("offer contains no filename")?;

    let target = download_dir.join(filename);
    if target.try_exists()? {
        anyhow::bail!("file {} already exists", target.display())
    }

    println!("Starting download.");
    let downloader = store.downloader(endpoint);
    let mut stream = downloader
        .download(offer.ticket.hash(), Some(offer.ticket.addr().id))
        .stream()
        .await
        .context("failed to download")?;

    while let Some(item) = stream.next().await {
        match item {
            DownloadProgressItem::Progress(bytes) => {
                let percentage = ((bytes as f32) / (offer.filesize as f32)) * 100.0;
                println!("downloaded {:?} percent", percentage)
            }
            DownloadProgressItem::Error(error) => anyhow::bail!("download failed {error}"),
            DownloadProgressItem::DownloadError => anyhow::bail!("download failed"),
            _ => (),
        }
    }
    println!("Finished download.");

    println!("Copying to destination.");

    store
        .blobs()
        .export(offer.ticket.hash(), target)
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

pub async fn confirm(offer: &Offer, send: &mut SendStream) -> std::io::Result<bool> {
    println!(
        "{} ({} Bytes) accept? [y/n]",
        offer.filename, offer.filesize
    );

    let mut answer = String::new();
    BufReader::new(tokio::io::stdin())
        .read_line(&mut answer)
        .await?;

    let decision = matches!(answer.trim().to_ascii_lowercase().as_str(), "y");
    send.write_u8(u8::from(decision)).await?;
    Ok(decision)
}
