use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh::{
    Endpoint,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_blobs::{api::downloader::DownloadProgressItem, store::mem::MemStore};
use n0_error::e;
use n0_future::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
};

use crate::protocol::{DecisionStatus, DownloadStatus, Offer};

// multiple offers are send via mpsc, each OfferRequest contains a sender
// to send exactly one answer
pub type OfferRequest = (Offer, oneshot::Sender<bool>);

#[derive(Debug, Clone)]
pub struct OfferProtocol {
    pub endpoint: Endpoint,
    pub store: MemStore,
    pub download_dir: PathBuf,
    pub offer_tx: mpsc::Sender<OfferRequest>,
}

impl OfferProtocol {
    pub fn new(
        endpoint: &Endpoint,
        store: &MemStore,
        download_dir: &Path,
        offer_tx: mpsc::Sender<OfferRequest>,
    ) -> Self {
        Self {
            endpoint: endpoint.clone(),
            store: store.clone(),
            download_dir: download_dir.to_path_buf(),
            offer_tx,
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

        let (decision_tx, decision_rx) = oneshot::channel();
        self.offer_tx
            .send((offer.clone(), decision_tx))
            .await
            .context("failed to send offer to UI")
            .map_err(accept_error)?;

        match decision_rx.await {
            Ok(true) => {
                send.write_u8(DecisionStatus::Accepted as u8).await?;

                if let Err(e) =
                    download(&self.endpoint, &self.store, &self.download_dir, offer).await
                {
                    let _ = send.write_u8(DownloadStatus::Failed as u8).await;
                    let _ = send.finish();
                    connection.closed().await;
                    return Err(accept_error(e));
                }

                send.write_u8(DownloadStatus::Completed as u8)
                    .await
                    .context("Failed to send download byte")
                    .map_err(accept_error)?;
                send.finish()?;
                connection.closed().await;
                Ok(())
            }
            Ok(false) | Err(_) => {
                println!("No transfer executed.");
                let _ = send.write_u8(DecisionStatus::Declined as u8).await;
                let _ = send.finish();
                connection.closed().await;
                Err(e!(AcceptError::NotAllowed))
            }
        }
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
    Ok(())
}
