use std::path::{Path, PathBuf};

use crate::protocol::{DecisionStatus, DownloadStatus, Offer};
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
    sync::{mpsc, oneshot, watch},
};

#[derive(Debug, Clone, Default)]
pub struct DownloadProgress {
    pub downloaded: u64,
    pub total: u64,
}

pub enum OfferDecision {
    Accept(PathBuf),
    Decline,
}

pub type OfferRequest = (Offer, oneshot::Sender<OfferDecision>);

#[derive(Debug, Clone)]
pub struct OfferProtocol {
    pub endpoint: Endpoint,
    pub store: MemStore,
    pub offer_tx: mpsc::Sender<OfferRequest>,
    pub progress_tx: watch::Sender<DownloadProgress>,
}

impl OfferProtocol {
    pub fn new(
        endpoint: &Endpoint,
        store: &MemStore,
        offer_tx: mpsc::Sender<OfferRequest>,
        progress_tx: watch::Sender<DownloadProgress>,
    ) -> Self {
        Self {
            endpoint: endpoint.clone(),
            store: store.clone(),
            offer_tx,
            progress_tx,
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
            Ok(OfferDecision::Accept(download_dir)) => {
                send.write_u8(DecisionStatus::Accepted as u8).await?;

                if let Err(e) = download(
                    &self.endpoint,
                    &self.store,
                    &download_dir,
                    offer,
                    &self.progress_tx,
                )
                .await
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
            Ok(OfferDecision::Decline) | Err(_) => {
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
    progress: &watch::Sender<DownloadProgress>,
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
                progress.send(DownloadProgress {
                    downloaded: bytes,
                    total: offer.filesize,
                })?;
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

    progress.send_replace(DownloadProgress {
        downloaded: offer.filesize,
        total: offer.filesize,
    });

    println!("Finished copying.");
    Ok(())
}
