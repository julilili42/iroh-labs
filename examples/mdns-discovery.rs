use iroh::{Endpoint, endpoint::presets};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;

#[tokio::main]
async fn main() {
    let endpoint = Endpoint::bind(presets::Minimal).await.unwrap();

    let mdns = MdnsAddressLookup::builder().build(endpoint.id()).unwrap();
    endpoint.address_lookup().unwrap().add(mdns.clone());

    let mut events = mdns.subscribe().await;
    while let Some(event) = events.next().await {
        match event {
            DiscoveryEvent::Discovered { endpoint_info, .. } => {
                println!("MDNS discovered: {:?}", endpoint_info);
            }
            DiscoveryEvent::Expired { endpoint_id } => {
                println!("MDNS expired: {endpoint_id}");
            }
            _ => {}
        }
    }
}
