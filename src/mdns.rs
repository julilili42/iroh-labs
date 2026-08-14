use anyhow::Result;
use anyhow::bail;
use iroh::Endpoint;
use iroh::EndpointAddr;
use iroh_mdns_address_lookup::MdnsAddressLookup;

use iroh_mdns_address_lookup::DiscoveryEvent;
use n0_future::StreamExt;

pub fn enable(endpoint: &Endpoint) -> Result<MdnsAddressLookup> {
    let mdns = MdnsAddressLookup::builder()
        .service_name("iroh-airdrop")
        .build(endpoint.id())?;

    endpoint.address_lookup()?.add(mdns.clone());
    Ok(mdns)
}

pub async fn discover_one(mdns: &MdnsAddressLookup) -> Result<EndpointAddr> {
    let mut event = mdns.subscribe().await;
    while let Some(event) = event.next().await {
        if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
            return Ok(endpoint_info.into_endpoint_addr());
        }
    }

    bail!("mDNS discovery stopped")
}
