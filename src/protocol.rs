use anyhow::{Context, Result, ensure};
use iroh::endpoint::{RecvStream, SendStream};
use iroh_blobs::ticket::BlobTicket;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const ALPN: &[u8] = b"iroh-share/transfer-offer/2";

#[derive(Debug, Clone)]
pub struct Offer {
    pub filename: String,
    pub filesize: u64,
    pub ticket: BlobTicket,
}

impl Offer {
    pub fn new(filename: &str, filesize: u64, ticket: &BlobTicket) -> Self {
        Self {
            filename: filename.to_string(),
            filesize,
            ticket: ticket.clone(),
        }
    }
}

impl Offer {
    pub async fn read_from(recv: &mut RecvStream) -> Result<Offer> {
        let filename_len = recv.read_u16().await? as usize;
        ensure!(filename_len <= 255, "filename too long");

        let mut filename = vec![0; filename_len];
        recv.read_exact(&mut filename).await?;

        let filename = String::from_utf8(filename).context("filename is not valid utf8")?;

        let filesize = recv.read_u64().await?;

        let ticket = recv.read_to_end(4096).await?;
        let ticket: BlobTicket = String::from_utf8(ticket)
            .context("ticket is not valid utf8")?
            .parse()
            .context("invalid blob ticket")?;

        Ok(Offer {
            filename,
            filesize,
            ticket,
        })
    }

    pub async fn write_to(&self, send: &mut SendStream) -> Result<()> {
        let filename = self.filename.as_bytes();
        let ticket = self.ticket.to_string();

        // need prefix, s.t. receiver knows how long each field is
        send.write_u16(filename.len().try_into()?).await?;
        send.write_all(filename).await?;

        send.write_u64(self.filesize).await?;
        send.write_all(ticket.as_bytes()).await?;
        send.finish()?;

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum DecisionStatus {
    Declined = 0,
    Accepted = 1,
}
pub enum DownloadStatus {
    Completed = 2,
    Failed = 3,
}

impl TryFrom<u8> for DecisionStatus {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            0 => Ok(DecisionStatus::Declined),
            1 => Ok(DecisionStatus::Accepted),
            _ => anyhow::bail!("invalid transfer status"),
        }
    }
}
impl TryFrom<u8> for DownloadStatus {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            2 => Ok(DownloadStatus::Completed),
            3 => Ok(DownloadStatus::Failed),
            _ => anyhow::bail!("invalid transfer status"),
        }
    }
}

pub async fn transfer_decision(recv: &mut RecvStream) -> Result<DecisionStatus> {
    let byte = recv
        .read_u8()
        .await
        .context("accept byte was not received")?;

    DecisionStatus::try_from(byte)
}

pub async fn download_finished(recv: &mut RecvStream) -> Result<DownloadStatus> {
    let byte = recv
        .read_u8()
        .await
        .context("download byte was not received")?;

    let status = DownloadStatus::try_from(byte)?;
    Ok(status)
}
