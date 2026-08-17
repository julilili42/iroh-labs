use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::receiver::OfferRequest;
use anyhow::Result;
use eframe::egui;
use iroh::{EndpointAddr, endpoint_info::UserData};
use tokio::sync::{mpsc, watch};

pub fn run(
    peer_rx: watch::Receiver<Vec<(UserData, EndpointAddr)>>,
    offer_rx: mpsc::Receiver<OfferRequest>,
) -> Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([640.0, 240.0]) // wide enough for the drag-drop overlay text
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "Iroh Drop",
        options,
        Box::new(|_| {
            Ok(Box::new(App {
                peer_rx,
                offer_rx,
                peers: Vec::new(),
                pending_offers: None,
                dropped_files: Vec::new(),
                picked_path: None,
                peer_pulse_started: HashMap::new(),
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
    dropped_files: Vec<egui::DroppedFileHandle>,
    picked_path: Option<String>,
    peer_pulse_started: HashMap<String, Instant>,
}

impl App {
    fn refresh_peers(&mut self) {
        if self.peer_rx.has_changed().unwrap_or(false) {
            self.peers = self.peer_rx.borrow_and_update().clone();
            self.peer_pulse_started
                .retain(|name, _| self.peers.iter().any(|(peer, _)| peer.as_ref() == name));
        }
    }
    fn refresh_offers(&mut self) {
        if self.pending_offers.is_none()
            && let Ok(request) = self.offer_rx.try_recv()
        {
            self.pending_offers = Some(request)
        }
    }
    fn show_pending_offers(&mut self, ui: &mut egui::Ui) {
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
    fn show_file_picker(&mut self, ui: &mut egui::Ui) {
        ui.label("Drag-and-drop files onto the window!");

        if ui.button("Open file…").clicked()
            && let Some(path) = rfd::FileDialog::new().pick_file()
        {
            self.picked_path = Some(path.display().to_string());
        }

        if let Some(picked_path) = &self.picked_path {
            ui.horizontal(|ui| {
                ui.label("Picked file:");
                ui.monospace(picked_path);
            });
        }
    }
    fn show_files_dropped(&mut self, ui: &mut egui::Ui) {
        // Show dropped files (if any):
        if !self.dropped_files.is_empty() {
            ui.group(|ui| {
                ui.label("Dropped files:");

                for file in &self.dropped_files {
                    #[cfg(not(target_arch = "wasm32"))]
                    ui.label(file.path().display().to_string());

                    #[cfg(target_arch = "wasm32")]
                    {
                        let Some(web_file) = file.web_file() else {
                            continue;
                        };
                        let name = web_file.name();
                        let mime = web_file.type_();
                        let size = web_file.size();
                        if mime.is_empty() {
                            ui.label(format!("{name} ({size} bytes)"));
                        } else {
                            ui.label(format!("{name} (type: {mime}, {size} bytes)"));
                        }
                    }
                }
            });
        }
    }
    fn show_peers(&mut self, ui: &mut egui::Ui) {
        const CIRCLE_DIAMETER: f32 = 100.0;
        const TEXT_TOLERANCE: f32 = 20.0;
        const PULSE_SIZE: f32 = 25.0;
        const PULSE_DURATION: f32 = 5.0;
        const FADE_DURATION: f32 = 1.0;

        if self.peers.is_empty() {
            ui.label("Searching in local net...");
        } else {
            for (name, _) in &self.peers {
                let name = name.to_string();
                let initial = name.chars().next().unwrap_or('?').to_string();
                let text_width = CIRCLE_DIAMETER + TEXT_TOLERANCE;
                let element_width = CIRCLE_DIAMETER + PULSE_SIZE * 2.0;
                let elapsed = self
                    .peer_pulse_started
                    .entry(name.clone())
                    .or_insert_with(Instant::now)
                    .elapsed();
                let elapsed_secs = elapsed.as_secs_f32();
                let fade = ((PULSE_DURATION - elapsed_secs) / FADE_DURATION).clamp(0.0, 1.0);

                ui.vertical(|ui| {
                    ui.allocate_ui(egui::vec2(element_width, 0.0), |ui| {
                        ui.vertical_centered(|ui| {
                            let (rect, _response) = ui.allocate_exact_size(
                                egui::vec2(element_width, CIRCLE_DIAMETER),
                                egui::Sense::hover(),
                            );

                            for (delay, pulse_size) in [(0.0, 17.0), (0.4, 21.0), (0.8, 25.0)] {
                                let wave_age = elapsed_secs - delay;
                                if wave_age >= 0.0 && fade > 0.0 {
                                    let pulse = (wave_age / 1.2) % 1.0;
                                    ui.painter().circle_filled(
                                        rect.center(),
                                        CIRCLE_DIAMETER / 2.0 + pulse_size * pulse,
                                        egui::Color32::from_white_alpha(
                                            (40.0 * (1.0 - pulse) * fade) as u8,
                                        ),
                                    );
                                }
                            }
                            ui.painter().circle(
                                rect.center(),
                                CIRCLE_DIAMETER / 2.0,
                                egui::Color32::WHITE,
                                egui::Stroke::new(2.0, egui::Color32::WHITE),
                            );
                            ui.painter().text(
                                rect.center(),
                                egui::Align2::CENTER_CENTER,
                                &initial,
                                egui::TextStyle::Heading.resolve(ui.style()),
                                egui::Color32::BLACK,
                            );

                            ui.add_space(5.0);

                            ui.allocate_ui_with_layout(
                                egui::vec2(text_width, 0.0),
                                egui::Layout::top_down(egui::Align::Center),
                                |ui| {
                                    ui.add(
                                        egui::Label::new(egui::RichText::new(&name))
                                            .wrap()
                                            .halign(egui::Align::Center),
                                    );
                                },
                            );
                        });
                    });
                });
            }
            if self
                .peer_pulse_started
                .values()
                .any(|started| started.elapsed() < Duration::from_secs(5))
            {
                ui.ctx().request_repaint();
            }
        }
    }
    fn collect_dropped_files(&mut self, ui: &mut egui::Ui) {
        ui.input(|i| {
            if !i.raw.dropped_files.is_empty() {
                self.dropped_files.clone_from(&i.raw.dropped_files);
            }
        });
    }
    fn preview_files_being_dropped(&mut self, ctx: &egui::Context) {
        use core::fmt::Write as _;
        use egui::{Align2, Color32, Id, LayerId, Order, TextStyle};

        if !ctx.input(|i| i.raw.hovered_files.is_empty()) {
            let text = ctx.input(|i| {
                let mut text = "Dropping files:\n".to_owned();
                for file in &i.raw.hovered_files {
                    if let Some(path) = &file.path {
                        write!(text, "\n{}", path.display()).ok();
                    } else if file.mime.is_empty() {
                        text += "\n???";
                    } else {
                        write!(text, "\n{}", file.mime).ok();
                    }
                }
                text
            });

            let painter =
                ctx.layer_painter(LayerId::new(Order::Foreground, Id::new("file_drop_target")));

            let content_rect = ctx.content_rect();
            painter.rect_filled(content_rect, 0.0, Color32::from_black_alpha(192));
            painter.text(
                content_rect.center(),
                Align2::CENTER_CENTER,
                text,
                TextStyle::Heading.resolve(&ctx.global_style()),
                Color32::WHITE,
            );
        }
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ui, |ui| {
            self.show_file_picker(ui);
            self.show_files_dropped(ui);

            self.refresh_peers();
            self.refresh_offers();
            ui.ctx().request_repaint_after(Duration::from_millis(250));
            self.show_pending_offers(ui);
            self.show_peers(ui);
        });

        self.preview_files_being_dropped(ui.ctx());
        self.collect_dropped_files(ui);
        // Collect dropped files:
    }
}
