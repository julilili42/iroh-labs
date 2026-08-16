use std::time::Duration;

use anyhow::{Context, Result};
use iroh::{EndpointAddr, endpoint_info::UserData};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::watch::Receiver,
    time,
};

use crate::protocol::Offer;

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
    println!("    cargo run -- send <FILE>");
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
