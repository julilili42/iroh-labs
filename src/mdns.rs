use std::collections::HashMap;

use anyhow::{Result, bail};
use iroh::{Endpoint, EndpointAddr, endpoint_info::UserData};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use tokio::sync::watch;

pub fn enable(endpoint: &Endpoint, device_name: &str) -> Result<MdnsAddressLookup> {
    let user_data = UserData::try_from(device_name.to_string())?;
    endpoint.set_user_data_for_address_lookup(Some(user_data));

    let mdns = MdnsAddressLookup::builder()
        .service_name("iroh-airdrop")
        .build(endpoint.id())?;

    endpoint.address_lookup()?.add(mdns.clone());
    Ok(mdns)
}

pub async fn discover(
    mdns: MdnsAddressLookup,
    tx: watch::Sender<Vec<(UserData, EndpointAddr)>>,
) -> Result<()> {
    let mut event = mdns.subscribe().await;
    let mut peers = HashMap::new();

    while let Some(event) = event.next().await {
        match event {
            DiscoveryEvent::Discovered { endpoint_info, .. } => {
                let id = endpoint_info.endpoint_id;
                let addr = endpoint_info.clone().into_endpoint_addr();
                if let Some(name) = endpoint_info.user_data() {
                    peers.insert(id, (name.clone(), addr));
                    tx.send(peers.values().cloned().collect())?;
                }
            }
            DiscoveryEvent::Expired { endpoint_id } => {
                peers.remove(&endpoint_id);
                tx.send(peers.values().cloned().collect())?;
            }
            _ => continue,
        }
    }

    bail!("mDNS discovery stopped")
}
