use anyhow::Result;

use crate::blob::{run_receiver, run_sender, start_iroh};

mod blob;
mod mdns;
mod protocol;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    // Grab all passed in arguments, the first one is the binary itself, so we skip it.
    let args: Vec<String> = std::env::args().skip(1).collect();
    // Convert to &str, so we can pattern-match easily:
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    let (endpoint, store, router) = start_iroh().await?;

    match arg_refs.as_slice() {
        ["send", filename] => run_sender(filename.to_string(), endpoint, router, &store).await?,
        ["receive"] => run_receiver(endpoint, router).await?,
        _ => {
            println!("Couldn't parse command line arguments: {args:?}");
            println!("Usage:");
            println!("    # to send:");
            println!("    cargo run -- send [FILE]");
            println!("    # this will print a ticket.");
            println!();
            println!("    # to receive:");
            println!("    cargo run -- receive [TICKET] [FILE]");
        }
    }

    Ok(())
}
