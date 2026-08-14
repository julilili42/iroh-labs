use std::{env, result::Result::Ok};

use anyhow::{Result, anyhow};
use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_ping::Ping;
use iroh_services::Client;
use iroh_tickets::{Ticket, endpoint::EndpointTicket};

async fn run_receiver() -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    let _services_client = match Client::builder(&endpoint).api_secret_from_env() {
        Ok(builder) => {
            let client = builder.name("ping-receiver")?.build().await?;
            println!("Connected to Iroh Services");
            Some(client)
        }
        Err(_) => None,
    };

    let ping = Ping::new();
    let ticket = EndpointTicket::new(endpoint.addr());
    println!("{ticket}");

    let _router = Router::builder(endpoint)
        .accept(iroh_ping::ALPN, ping)
        .spawn();

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_sender(ticket: EndpointTicket) -> Result<()> {
    let send_ep = Endpoint::bind(presets::N0).await?;
    let sender_ping = Ping::new();
    let rtt = sender_ping
        .ping(&send_ep, ticket.endpoint_addr().clone())
        .await?;
    println!("ping took: {:?} to complete", rtt);
    send_ep.close().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let mut args = env::args().skip(1);
    let role = args
        .next()
        .ok_or_else(|| anyhow!("expected 'receiver' or 'sender' as the first argument"))?;

    match role.as_str() {
        "receiver" => run_receiver().await,
        "sender" => {
            let ticket_str = args
                .next()
                .ok_or_else(|| anyhow!("expected ticket as the second argument"))?;
            let ticket = EndpointTicket::decode_string(&ticket_str)
                .map_err(|e| anyhow!("failed to parse ticket: {}", e))?;
            run_sender(ticket).await
        }
        _ => Err(anyhow!(
            "unknown role '{}'; use 'receiver' or 'sender'",
            role
        )),
    }
}
