use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket};
use n0_error::e;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::blob::download;

pub const ALPN: &[u8] = b"iroh-labs/transfer-offer/0";

fn accept_error(error: anyhow::Error) -> AcceptError {
    AcceptError::from_boxed(error.into_boxed_dyn_error())
}

async fn confirm(offer: &Offer) -> std::io::Result<bool> {
    println!(
        "{} ({} Bytes) annehmen? [y/n]",
        offer.filename, offer.filesize
    );

    let mut answer = String::new();
    BufReader::new(tokio::io::stdin())
        .read_line(&mut answer)
        .await?;

    Ok(matches!(answer.trim().to_ascii_lowercase().as_str(), "y"))
}

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
    async fn read_from(recv: &mut RecvStream) -> Result<Offer> {
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

    async fn write_to(&self, send: &mut SendStream) -> Result<()> {
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

#[derive(Debug, Clone)]
pub struct OfferProtocol {
    pub endpoint: Endpoint,
    pub store: MemStore,
}

impl OfferProtocol {
    pub fn new(endpoint: &Endpoint, store: &MemStore) -> Self {
        Self {
            endpoint: endpoint.clone(),
            store: store.clone(),
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

        let accepted = confirm(&offer).await.map_err(AcceptError::from_err)?;

        if accepted {
            download(&self.endpoint, &self.store, offer)
                .await
                .map_err(accept_error)?;
        }
        send.write_u8(u8::from(accepted)).await?;
        send.finish()?;
        Ok(())
    }
}

pub async fn send_transfer_offer(
    endpoint: &Endpoint,
    addr: EndpointAddr,
    offer: &Offer,
) -> Result<bool> {
    // open connection to the receiving endpoint
    let conn = endpoint.connect(addr, ALPN).await?;

    // receiver must be able to accept / decline transfer
    let (mut send, mut recv) = conn.open_bi().await?;

    offer.write_to(&mut send).await?;

    Ok(match recv.read_u8().await? {
        0 => false,
        1 => true,
        value => anyhow::bail!("invalid response {value}"),
    })
}
