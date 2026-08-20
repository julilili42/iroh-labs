use std::{
    env,
    path::{self, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use iroh::{EndpointAddr, endpoint_info::UserData};
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::watch::Receiver,
    time,
};

use crate::protocol::Offer;

pub enum Command {
    UI,
    Send {
        filename: String,
        endpoint_addr: Option<EndpointAddr>,
    },
    Receive {
        download_dir: PathBuf,
    },
}

pub fn parse_arguments() -> Result<Option<Command>> {
    // Grab all passed in arguments, the first one is the binary itself, so we skip it.
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Convert to &str, so we can pattern-match easily:
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    match arg_refs.as_slice() {
        [] => Ok(Some(Command::UI)),
        ["send", filename, ticket_str] => {
            let ticket = EndpointTicket::decode_string(ticket_str)
                .map_err(|e| anyhow!("failed to parse ticket: {}", e))?;
            let endpoint_addr = ticket.endpoint_addr().clone();

            Ok(Some(Command::Send {
                filename: filename.to_string(),
                endpoint_addr: Some(endpoint_addr),
            }))
        }
        ["send", filename] => Ok(Some(Command::Send {
            filename: filename.to_string(),
            endpoint_addr: None,
        })),
        ["receive"] => Ok(Some(Command::Receive {
            download_dir: env::current_dir()?,
        })),
        ["receive", download_dir] => Ok(Some(Command::Receive {
            download_dir: path::absolute(download_dir)?,
        })),
        [..] => Ok(None),
    }
}

pub async fn select_receiver(
    mut rx: Receiver<Vec<(UserData, EndpointAddr)>>,
) -> Result<EndpointAddr> {
    let search_str = "Searching in local net...";
    println!("{}", search_str);
    rx.wait_for(|devices| !devices.is_empty())
        .await
        .context("device discovery stopped")?;

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

    let mut stdin = BufReader::new(io::stdin());
    stdin.read_line(&mut input).await?;

    let idx = input
        .trim()
        .parse::<usize>()
        .context("selection must be a number")?;

    let (_, endpoint_addr) = idx
        .checked_sub(1)
        .and_then(|idx| devices.get(idx))
        .context("selection out of range")?;

    Ok(endpoint_addr.clone())
}

pub fn print_usage() {
    println!("Usage:");
    println!("    cargo run -- send <FILE> [TICKET]");
    println!("    cargo run -- receive [DOWNLOAD_DIR]");
}

pub async fn confirm(offer: &Offer) -> io::Result<bool> {
    println!(
        "{} ({} Bytes) accept? [y/n]",
        offer.filename, offer.filesize
    );

    let mut answer = String::new();
    BufReader::new(io::stdin()).read_line(&mut answer).await?;

    let decision = matches!(answer.trim().to_ascii_lowercase().as_str(), "y");
    Ok(decision)
}
