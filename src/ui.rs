use std::time::Duration;

use crate::receiver::OfferRequest;
use anyhow::Result;
use eframe::{NativeOptions, egui, run_native};
use iroh::{EndpointAddr, endpoint_info::UserData};
use tokio::sync::{mpsc, watch};

pub fn run(
    peer_rx: watch::Receiver<Vec<(UserData, EndpointAddr)>>,
    offer_rx: mpsc::Receiver<OfferRequest>,
) -> Result<()> {
    run_native(
        "Iroh Drop",
        NativeOptions::default(),
        Box::new(|_| {
            Ok(Box::new(App {
                peer_rx,
                offer_rx,
                peers: Vec::new(),
                pending_offers: None,
            }) as Box<dyn eframe::App>)
        }),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))
}

struct App {
    peer_rx: watch::Receiver<Vec<(UserData, EndpointAddr)>>,
    offer_rx: mpsc::Receiver<OfferRequest>,
    peers: Vec<(UserData, EndpointAddr)>,
    pending_offers: Option<OfferRequest>,
}

impl App {
    fn refresh_peers(&mut self) {
        if self.peer_rx.has_changed().unwrap_or(false) {
            self.peers = self.peer_rx.borrow_and_update().clone()
        }
    }
    fn refresh_offers(&mut self) {
        if self.pending_offers.is_none()
            && let Ok(request) = self.offer_rx.try_recv()
        {
            self.pending_offers = Some(request)
        }
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _: &mut eframe::Frame) {
        self.refresh_peers();
        self.refresh_offers();
        ui.ctx().request_repaint_after(Duration::from_millis(250));

        if self.peers.is_empty() {
            ui.label("Searching in local net...");
        } else {
            for (name, _) in &self.peers {
                ui.label(name.to_string());
            }
        }

        let decision = self.pending_offers.as_ref().and_then(|(offer, _)| {
            ui.label(&offer.filename);
            ui.label(format!("{} bytes", offer.filesize));

            if ui.button("Accept").clicked() {
                Some(true)
            } else if ui.button("Decline").clicked() {
                Some(false)
            } else {
                None
            }
        });

        if let Some(decision) = decision
            && let Some((_, decision_tx)) = self.pending_offers.take()
        {
            let _ = decision_tx.send(decision);
        }
    }
}
