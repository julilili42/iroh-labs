use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    Runtime,
    receiver::{OfferDecision, OfferRequest},
    sender::{SendOutcome, run_sender},
};
use anyhow::Result;
use eframe::egui::{self, TextBuffer};
use iroh::{Endpoint, EndpointAddr, endpoint_info::UserData};
use iroh_blobs::store::mem::MemStore;
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use tokio::sync::{mpsc, watch};

pub fn run(runtime: Runtime) -> Result<()> {
    let (send_result_tx, send_result_rx) = mpsc::unbounded_channel();
    let (send_progress_tx, send_progress_rx) = watch::channel(0_u64);
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([560.0, 300.0])
            .with_min_inner_size([500.0, 300.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "Iroh Share",
        options,
        Box::new(|cc| {
            cc.egui_ctx
                .all_styles_mut(|style| style.visuals.weak_text_alpha = 0.7);
            Ok(Box::new(App {
                peer_rx: runtime.peer_rx,
                offer_rx: runtime.offer_rx,
                progress_rx: runtime.progress_rx,
                peers: Vec::new(),
                pending_offers: None,
                dropped_files: Vec::new(),
                picked_path: None,
                peer_pulse_started: HashMap::new(),
                selected_peer: None,
                display_name: whoami::devicename().or_else(|_| whoami::hostname())?,
                endpoint: runtime.endpoint,
                ticket: runtime.ticket,
                store: runtime.store,
                runtime: tokio::runtime::Handle::current(),
                send_result_tx,
                send_result_rx,
                send_progress_tx,
                send_progress_rx,
                send_total: 0,
                send_status: None,
                ticket_copied_at: None,
                ticket_input: None,
                ticket_error: false,
                receive_status: None,
            }) as Box<dyn eframe::App>)
        }),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))
}

struct App {
    peer_rx: watch::Receiver<Vec<(UserData, EndpointAddr)>>,
    offer_rx: mpsc::Receiver<OfferRequest>,
    progress_rx: watch::Receiver<u64>,
    peers: Vec<(UserData, EndpointAddr)>,
    pending_offers: Option<OfferRequest>,
    dropped_files: Vec<egui::DroppedFileHandle>,
    picked_path: Option<String>,
    peer_pulse_started: HashMap<String, Instant>,
    selected_peer: Option<(String, EndpointAddr)>,
    display_name: String,
    endpoint: Endpoint,
    ticket: EndpointTicket,
    store: MemStore,
    runtime: tokio::runtime::Handle,
    send_result_tx: mpsc::UnboundedSender<Result<SendOutcome, String>>,
    send_result_rx: mpsc::UnboundedReceiver<Result<SendOutcome, String>>,
    send_progress_tx: watch::Sender<u64>,
    send_progress_rx: watch::Receiver<u64>,
    send_total: u64,
    send_status: Option<SendStatus>,
    ticket_copied_at: Option<Instant>,
    ticket_input: Option<String>,
    ticket_error: bool,
    receive_status: Option<ReceiveStatus>,
}

enum SendStatus {
    Sending,
    Completed,
    Declined,
    Failed,
}

struct ReceiveStatus {
    filename: String,
    downloaded: u64,
    total: u64,
    completed_at: Option<Instant>,
}

fn download_fraction(downloaded: u64, total: u64) -> f32 {
    if total == 0 {
        0.0
    } else {
        (downloaded as f64 / total as f64).clamp(0.0, 1.0) as f32
    }
}

impl App {
    fn action_button(ui: &egui::Ui, label: &'static str, primary: bool) -> egui::Button<'static> {
        let (text_color, fill, stroke) = if primary {
            (
                egui::Color32::WHITE,
                egui::Color32::from_rgb(112, 103, 255),
                egui::Stroke::NONE,
            )
        } else {
            (
                ui.visuals().text_color(),
                ui.visuals().faint_bg_color,
                ui.visuals().widgets.inactive.bg_stroke,
            )
        };

        egui::Button::new(egui::RichText::new(label).size(15.0).color(text_color))
            .fill(fill)
            .stroke(stroke)
            .corner_radius(8.0)
            .min_size(egui::vec2(92.0, 34.0))
    }

    fn refresh_peers(&mut self) {
        if self.peer_rx.has_changed().unwrap_or(false) {
            self.peers = self.peer_rx.borrow_and_update().clone();
            self.peer_pulse_started
                .retain(|name, _| self.peers.iter().any(|(peer, _)| peer.as_ref() == name));
            if self.selected_peer.as_ref().is_some_and(|(_, selected)| {
                !self.peers.iter().any(|(_, peer)| peer.id == selected.id)
            }) {
                self.selected_peer = None;
            }
        }
    }
    fn refresh_offers(&mut self) {
        if self.pending_offers.is_none()
            && let Ok(request) = self.offer_rx.try_recv()
        {
            self.pending_offers = Some(request)
        }
    }
    fn refresh_send_status(&mut self) {
        if let Ok(result) = self.send_result_rx.try_recv() {
            self.send_status = Some(match result {
                Ok(SendOutcome::Completed) => SendStatus::Completed,
                Ok(SendOutcome::Declined) => SendStatus::Declined,
                Err(_) => SendStatus::Failed,
            });
        }
    }
    fn refresh_receive_status(&mut self) {
        let Some(status) = &mut self.receive_status else {
            return;
        };
        if self.progress_rx.has_changed().unwrap_or(false) {
            status.downloaded = *self.progress_rx.borrow_and_update();
            if status.total > 0 && status.downloaded >= status.total {
                status.completed_at.get_or_insert_with(Instant::now);
            }
        }
        if status
            .completed_at
            .is_some_and(|at| at.elapsed() >= Duration::from_secs(2))
        {
            self.receive_status = None;
        }
    }
    fn send_file(&mut self, path: std::path::PathBuf) {
        let Some((_, endpoint_addr)) = &self.selected_peer else {
            return;
        };
        if matches!(self.send_status, Some(SendStatus::Sending)) {
            return;
        }

        let endpoint = self.endpoint.clone();
        let endpoint_addr = endpoint_addr.clone();
        let store = self.store.clone();
        let result_tx = self.send_result_tx.clone();
        let progress_tx = self.send_progress_tx.clone();
        self.send_total = std::fs::metadata(&path).map_or(0, |metadata| metadata.len());
        self.send_progress_tx.send_replace(0);
        self.send_status = Some(SendStatus::Sending);
        self.runtime.spawn(async move {
            let result = run_sender(
                progress_tx,
                path.to_string_lossy().as_str(),
                &endpoint,
                &store,
                endpoint_addr,
            )
            .await
            .map_err(|error| error.to_string());
            let _ = result_tx.send(result);
        });
    }
    fn show_pending_offers(&mut self, ui: &mut egui::Ui) {
        let sender = self.pending_offers.as_ref().and_then(|(offer, _)| {
            self.peers
                .iter()
                .find(|(_, address)| address.id == offer.ticket.addr().id)
                .map(|(name, _)| name.to_string())
        });
        let decision = self.pending_offers.as_ref().and_then(|(offer, _)| {
            egui::Modal::new(egui::Id::new("incoming_file"))
                .frame(egui::Frame::popup(ui.style()).inner_margin(egui::Margin::symmetric(16, 14)))
                .show(ui.ctx(), |ui| {
                    ui.set_width(280.0);
                    ui.vertical(|ui| {
                        ui.label(egui::RichText::new("Incoming file").size(20.0).strong());
                        ui.label(
                            egui::RichText::new(format!(
                                "From “{}”",
                                sender.as_deref().unwrap_or("Unknown device")
                            ))
                            .size(14.0)
                            .color(ui.visuals().weak_text_color()),
                        );
                        ui.add_space(16.0);
                        egui::Frame::new()
                            .fill(ui.visuals().faint_bg_color)
                            .corner_radius(8.0)
                            .inner_margin(12.0)
                            .show(ui, |ui| {
                                ui.set_min_width(256.0);
                                ui.label(egui::RichText::new(&offer.filename).size(16.0).strong());
                                ui.label(
                                    egui::RichText::new(format!("{} bytes", offer.filesize))
                                        .size(13.0)
                                        .color(ui.visuals().weak_text_color()),
                                );
                            });
                        ui.add_space(16.0);
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.add(Self::action_button(ui, "Accept", true)).clicked()
                                && let Some(path) = rfd::FileDialog::new().pick_folder()
                            {
                                Some(OfferDecision::Accept(path))
                            } else if ui.add(Self::action_button(ui, "Decline", false)).clicked() {
                                Some(OfferDecision::Decline)
                            } else {
                                None
                            }
                        })
                        .inner
                    })
                    .inner
                })
                .inner
        });

        if let Some(decision) = decision
            && let Some((offer, decision_tx)) = self.pending_offers.take()
        {
            if matches!(&decision, OfferDecision::Accept(_)) {
                self.progress_rx.borrow_and_update();
                self.receive_status = Some(ReceiveStatus {
                    filename: offer.filename,
                    downloaded: 0,
                    total: offer.filesize,
                    completed_at: None,
                });
            }
            let _ = decision_tx.send(decision);
        }
    }
    fn show_header(&mut self, ui: &mut egui::Ui) {
        let selected_peer = self.selected_peer.clone();
        let mut go_back = false;
        ui.add_space(12.0);
        ui.horizontal(|ui| {
            ui.add_space(12.0);
            if selected_peer.is_some() {
                go_back = ui
                    .add_enabled(
                        !matches!(self.send_status, Some(SendStatus::Sending)),
                        egui::Button::new(egui::RichText::new("‹").size(28.0))
                            .frame(false)
                            .min_size(egui::vec2(32.0, 32.0)),
                    )
                    .on_hover_text("Back to nearby devices")
                    .clicked();
            } else {
                let (rect, _) =
                    ui.allocate_exact_size(egui::vec2(32.0, 32.0), egui::Sense::hover());
                let pulse = (ui.input(|input| input.time) as f32 / 1.6).fract();
                let color = egui::Color32::from_rgb(112, 103, 255);
                ui.painter().circle_filled(
                    rect.center(),
                    4.0 + 10.0 * pulse,
                    color.gamma_multiply(0.35 * (1.0 - pulse)),
                );
                ui.painter().circle_filled(rect.center(), 4.0, color);
                ui.ctx().request_repaint();
            }
            ui.vertical(|ui| {
                if let Some((peer, _)) = &selected_peer {
                    ui.label(egui::RichText::new("Sending").size(28.0).strong());
                    ui.label(
                        egui::RichText::new(format!("To “{peer}”"))
                            .size(18.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                } else {
                    ui.label(egui::RichText::new("Nearby").size(28.0).strong());
                    ui.label(
                        egui::RichText::new(format!("As “{}”", self.display_name))
                            .size(18.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                }
            });
            if selected_peer.is_none() {
                ui.allocate_ui_with_layout(
                    egui::vec2(ui.available_width(), 57.0),
                    egui::Layout::top_down(egui::Align::RIGHT),
                    |ui| {
                        let copied = self
                            .ticket_copied_at
                            .is_some_and(|at| at.elapsed() < Duration::from_secs(2));
                        let (rect, response) =
                            ui.allocate_exact_size(egui::vec2(116.0, 32.0), egui::Sense::click());
                        if response.hovered() {
                            ui.painter()
                                .rect_filled(rect, 4.0, ui.visuals().faint_bg_color);
                        }
                        ui.painter().text(
                            egui::pos2(rect.right() - 32.0, rect.center().y),
                            egui::Align2::RIGHT_CENTER,
                            "Copy Ticket",
                            egui::FontId::proportional(15.0),
                            ui.visuals().text_color(),
                        );
                        let center = egui::pos2(rect.right() - 16.0, rect.center().y);
                        if copied {
                            Self::paint_success_icon(ui, center, 13.0);
                        } else {
                            let stroke = egui::Stroke::new(2.0, ui.visuals().text_color());
                            ui.painter().rect_stroke(
                                egui::Rect::from_center_size(
                                    egui::pos2(center.x, center.y + 1.0),
                                    egui::vec2(15.0, 19.0),
                                ),
                                2.0,
                                stroke,
                                egui::StrokeKind::Inside,
                            );
                            ui.painter().line_segment(
                                [
                                    egui::pos2(center.x - 4.0, center.y - 10.0),
                                    egui::pos2(center.x + 4.0, center.y - 10.0),
                                ],
                                egui::Stroke::new(4.0, ui.visuals().text_color()),
                            );
                        }
                        if response
                            .on_hover_text(if copied {
                                "Ticket copied"
                            } else {
                                "Copy connection ticket"
                            })
                            .clicked()
                        {
                            ui.ctx().copy_text(self.ticket.to_string());
                            self.ticket_copied_at = Some(Instant::now());
                        }
                        let (rect, response) =
                            ui.allocate_exact_size(egui::vec2(116.0, 22.0), egui::Sense::click());
                        if response.hovered() {
                            ui.painter()
                                .rect_filled(rect, 4.0, ui.visuals().faint_bg_color);
                        }
                        ui.painter().text(
                            egui::pos2(rect.right() - 32.0, rect.center().y),
                            egui::Align2::RIGHT_CENTER,
                            "Use Ticket",
                            egui::FontId::proportional(15.0),
                            ui.visuals().text_color(),
                        );
                        let center = egui::pos2(rect.right() - 16.0, rect.center().y);
                        let stroke = egui::Stroke::new(1.7, ui.visuals().text_color());
                        ui.painter().line(
                            vec![
                                egui::pos2(center.x - 8.0, center.y - 6.0),
                                egui::pos2(center.x + 8.0, center.y - 6.0),
                                egui::pos2(center.x + 8.0, center.y - 2.5),
                                egui::pos2(center.x + 5.5, center.y),
                                egui::pos2(center.x + 8.0, center.y + 2.5),
                                egui::pos2(center.x + 8.0, center.y + 6.0),
                                egui::pos2(center.x - 8.0, center.y + 6.0),
                                egui::pos2(center.x - 8.0, center.y + 2.5),
                                egui::pos2(center.x - 5.5, center.y),
                                egui::pos2(center.x - 8.0, center.y - 2.5),
                                egui::pos2(center.x - 8.0, center.y - 6.0),
                            ],
                            stroke,
                        );
                        ui.painter().line_segment(
                            [
                                egui::pos2(center.x + 2.5, center.y - 4.5),
                                egui::pos2(center.x + 2.5, center.y - 1.5),
                            ],
                            stroke,
                        );
                        ui.painter().line_segment(
                            [
                                egui::pos2(center.x + 2.5, center.y + 1.5),
                                egui::pos2(center.x + 2.5, center.y + 4.5),
                            ],
                            stroke,
                        );
                        if response.on_hover_text("Send using a ticket").clicked() {
                            self.ticket_input = Some(String::new());
                            self.ticket_error = false;
                        }
                    },
                );
            }
        });
        if go_back {
            self.selected_peer = None;
            self.picked_path = None;
            self.dropped_files.clear();
            self.send_status = None;
        }
        ui.add_space(12.0);
    }
    fn show_ticket_modal(&mut self, ui: &mut egui::Ui) {
        if self.ticket_input.is_none() {
            return;
        }

        let action = egui::Modal::new(egui::Id::new("use_ticket"))
            .frame(egui::Frame::popup(ui.style()).inner_margin(egui::Margin::symmetric(16, 14)))
            .show(ui.ctx(), |ui| {
                ui.set_width(360.0);
                ui.label(egui::RichText::new("Send with ticket").size(20.0).strong());
                ui.add_space(10.0);
                let response = ui.add(
                    egui::TextEdit::singleline(self.ticket_input.as_mut().unwrap())
                        .hint_text("Paste ticket")
                        .desired_width(f32::INFINITY),
                );
                if response.changed() {
                    self.ticket_error = false;
                }
                if self.ticket_error {
                    ui.label(
                        egui::RichText::new("Invalid ticket").color(ui.visuals().error_fg_color),
                    );
                }
                ui.add_space(12.0);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.add(Self::action_button(ui, "Continue", true)).clicked() {
                        Some(true)
                    } else if ui.add(Self::action_button(ui, "Cancel", false)).clicked() {
                        Some(false)
                    } else {
                        None
                    }
                })
                .inner
            })
            .inner;

        match action {
            Some(true) => match EndpointTicket::decode_string(
                self.ticket_input.as_deref().unwrap_or_default().trim(),
            ) {
                Ok(ticket) => {
                    self.selected_peer =
                        Some(("Ticket device".to_owned(), ticket.endpoint_addr().clone()));
                    self.ticket_input = None;
                    self.ticket_error = false;
                }
                Err(_) => self.ticket_error = true,
            },
            Some(false) => {
                self.ticket_input = None;
                self.ticket_error = false;
            }
            None => {}
        }
    }
    fn show_file_picker(&mut self, ui: &mut egui::Ui) {
        let rect = ui
            .available_rect_before_wrap()
            .shrink2(egui::vec2(16.0, 8.0));
        let border = [
            rect.left_top(),
            rect.right_top(),
            rect.right_bottom(),
            rect.left_bottom(),
            rect.left_top(),
        ];

        ui.painter()
            .rect_filled(rect, 12.0, ui.visuals().faint_bg_color);
        ui.painter().extend(egui::Shape::dotted_line(
            &border,
            ui.visuals().weak_text_color(),
            7.0,
            1.5,
        ));

        let content_rect = egui::Rect::from_center_size(
            rect.center(),
            egui::vec2(
                (rect.width() - 32.0).max(0.0),
                (rect.height() - 12.0).max(0.0),
            ),
        );
        ui.scope_builder(
            egui::UiBuilder::new().max_rect(content_rect).layout(
                egui::Layout::top_down(egui::Align::Center).with_main_align(egui::Align::Center),
            ),
            |ui| {
                let (icon_rect, _) =
                    ui.allocate_exact_size(egui::vec2(48.0, 48.0), egui::Sense::hover());
                let center = icon_rect.center();
                let stroke = egui::Stroke::new(2.5, ui.visuals().text_color());
                ui.painter().line_segment(
                    [
                        egui::pos2(center.x, center.y + 8.0),
                        egui::pos2(center.x, center.y - 14.0),
                    ],
                    stroke,
                );
                ui.painter().line_segment(
                    [
                        egui::pos2(center.x, center.y - 14.0),
                        egui::pos2(center.x - 8.0, center.y - 6.0),
                    ],
                    stroke,
                );
                ui.painter().line_segment(
                    [
                        egui::pos2(center.x, center.y - 14.0),
                        egui::pos2(center.x + 8.0, center.y - 6.0),
                    ],
                    stroke,
                );
                ui.painter().line(
                    vec![
                        egui::pos2(center.x - 15.0, center.y + 6.0),
                        egui::pos2(center.x - 15.0, center.y + 16.0),
                        egui::pos2(center.x + 15.0, center.y + 16.0),
                        egui::pos2(center.x + 15.0, center.y + 6.0),
                    ],
                    stroke,
                );
                ui.add_space(8.0);
                ui.label(egui::RichText::new("Drop a file here").size(18.0).strong());
                ui.label(
                    egui::RichText::new("or choose one from your device")
                        .color(ui.visuals().weak_text_color()),
                );
                ui.add_space(12.0);

                let sending = matches!(self.send_status, Some(SendStatus::Sending));
                if ui
                    .add_enabled(!sending, Self::action_button(ui, "Choose file", true))
                    .clicked()
                    && let Some(path) = rfd::FileDialog::new().pick_file()
                {
                    self.dropped_files.clear();
                    self.picked_path = Some(path.display().to_string());
                    self.send_file(path);
                }
            },
        );
    }
    fn selected_filename(&self) -> String {
        self.picked_path
            .as_deref()
            .map(std::path::Path::new)
            .or_else(|| self.dropped_files.first().map(|file| file.path()))
            .and_then(std::path::Path::file_name)
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned()
    }
    fn show_transfer_icon(ui: &mut egui::Ui, symbol: &str, color: egui::Color32) {
        let (rect, _) = ui.allocate_exact_size(egui::vec2(42.0, 42.0), egui::Sense::hover());
        ui.painter()
            .circle_filled(rect.center(), 21.0, color.gamma_multiply(0.18));
        ui.painter().text(
            rect.center(),
            egui::Align2::CENTER_CENTER,
            symbol,
            egui::FontId::proportional(22.0),
            color,
        );
    }
    fn paint_success_icon(ui: &egui::Ui, center: egui::Pos2, radius: f32) {
        let color = egui::Color32::from_rgb(92, 190, 120);
        ui.painter()
            .circle_filled(center, radius, color.gamma_multiply(0.18));
        let scale = radius / 21.0;
        let stroke = egui::Stroke::new(2.5 * scale, color);
        ui.painter().line_segment(
            [
                egui::pos2(center.x - 9.0 * scale, center.y),
                egui::pos2(center.x - 3.0 * scale, center.y + 7.0 * scale),
            ],
            stroke,
        );
        ui.painter().line_segment(
            [
                egui::pos2(center.x - 3.0 * scale, center.y + 7.0 * scale),
                egui::pos2(center.x + 10.0 * scale, center.y - 8.0 * scale),
            ],
            stroke,
        );
    }
    fn show_success_icon(ui: &mut egui::Ui) {
        let (rect, _) = ui.allocate_exact_size(egui::vec2(42.0, 42.0), egui::Sense::hover());
        Self::paint_success_icon(ui, rect.center(), 21.0);
    }
    fn show_failure_icon(ui: &mut egui::Ui) {
        let (rect, _) = ui.allocate_exact_size(egui::vec2(42.0, 42.0), egui::Sense::hover());
        let color = egui::Color32::from_rgb(218, 94, 94);
        let center = rect.center();
        ui.painter()
            .circle_stroke(center, 18.0, egui::Stroke::new(1.8, color));
        ui.painter().text(
            egui::pos2(center.x, center.y + 1.0),
            egui::Align2::CENTER_CENTER,
            "!",
            egui::FontId::proportional(20.0),
            color,
        );
    }
    fn show_receive(&self, ui: &mut egui::Ui) {
        let Some(status) = &self.receive_status else {
            return;
        };
        let available = ui.available_rect_before_wrap();
        let content_rect =
            egui::Rect::from_center_size(available.center(), egui::vec2(available.width(), 120.0));
        ui.scope_builder(
            egui::UiBuilder::new()
                .max_rect(content_rect)
                .layout(egui::Layout::top_down(egui::Align::Center)),
            |ui| {
                if status.completed_at.is_some() {
                    Self::show_success_icon(ui);
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new("File received").size(20.0).strong());
                } else {
                    ui.label(egui::RichText::new("Receiving file").size(20.0).strong());
                    ui.label(
                        egui::RichText::new(&status.filename)
                            .size(15.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                    ui.add_space(12.0);
                    let fraction = download_fraction(status.downloaded, status.total);
                    let response = ui.add(
                        egui::ProgressBar::new(fraction)
                            .desired_width(280.0)
                            .desired_height(24.0),
                    );
                    ui.painter().text(
                        response.rect.center(),
                        egui::Align2::CENTER_CENTER,
                        format!("{}%", (fraction * 100.0) as u8),
                        egui::FontId::proportional(14.0),
                        ui.visuals().text_color(),
                    );
                }
            },
        );
    }
    fn show_transfer(&mut self, ui: &mut egui::Ui) {
        let filename = self.selected_filename();
        let mut reset = false;
        let available = ui.available_rect_before_wrap();
        let block_height: f32 = if matches!(self.send_status, Some(SendStatus::Sending)) {
            110.0
        } else {
            164.0
        };
        let content_rect = egui::Rect::from_center_size(
            available.center(),
            egui::vec2(available.width(), block_height.min(available.height())),
        );

        ui.scope_builder(
            egui::UiBuilder::new()
                .max_rect(content_rect)
                .layout(egui::Layout::top_down(egui::Align::Center)),
            |ui| match self.send_status.as_ref() {
                Some(SendStatus::Sending) => {
                    ui.label(egui::RichText::new("Transferring").size(20.0).strong());
                    ui.label(
                        egui::RichText::new(&filename)
                            .size(15.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                    ui.add_space(12.0);
                    let fraction =
                        download_fraction(*self.send_progress_rx.borrow(), self.send_total);
                    let response = ui.add(
                        egui::ProgressBar::new(fraction)
                            .desired_width(280.0)
                            .desired_height(24.0),
                    );
                    ui.painter().text(
                        response.rect.center(),
                        egui::Align2::CENTER_CENTER,
                        format!("{}%", (fraction * 100.0) as u8),
                        egui::FontId::proportional(14.0),
                        ui.visuals().text_color(),
                    );
                }
                Some(SendStatus::Completed) => {
                    Self::show_success_icon(ui);
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new("Transfer complete").size(20.0).strong());
                    ui.label(
                        egui::RichText::new(format!("“{filename}” was sent successfully."))
                            .size(14.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                    ui.add_space(16.0);
                    reset = ui
                        .add(Self::action_button(ui, "Send another file", true))
                        .clicked();
                }
                Some(SendStatus::Declined) => {
                    Self::show_transfer_icon(ui, "×", ui.visuals().warn_fg_color);
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new("Transfer declined").size(20.0).strong());
                    ui.label(
                        egui::RichText::new(format!("“{filename}” was declined by the receiver."))
                            .size(14.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                    ui.add_space(16.0);
                    reset = ui
                        .add(Self::action_button(ui, "Choose another file", true))
                        .clicked();
                }
                Some(SendStatus::Failed) => {
                    Self::show_failure_icon(ui);
                    ui.add_space(8.0);
                    ui.label(egui::RichText::new("Transfer failed").size(20.0).strong());
                    ui.label(
                        egui::RichText::new(&filename)
                            .size(15.0)
                            .color(ui.visuals().weak_text_color()),
                    );
                    ui.add_space(16.0);
                    reset = ui
                        .add(Self::action_button(ui, "Try another file", true))
                        .clicked();
                }
                None => {}
            },
        );

        if reset {
            self.picked_path = None;
            self.dropped_files.clear();
            self.send_progress_tx.send_replace(0);
            self.send_total = 0;
            self.send_status = None;
        }
    }
    fn show_peers(&mut self, ui: &mut egui::Ui) {
        if self.peers.is_empty() {
            let available = ui.available_rect_before_wrap();
            let rect = egui::Rect::from_center_size(
                available.center(),
                egui::vec2(available.width(), 100.0),
            );
            ui.scope_builder(
                egui::UiBuilder::new().max_rect(rect).layout(
                    egui::Layout::top_down(egui::Align::Center)
                        .with_main_align(egui::Align::Center),
                ),
                |ui| {
                    ui.label(
                        egui::RichText::new("No devices found")
                            .size(28.0)
                            .strong()
                            .extra_letter_spacing(-0.3),
                    );
                    ui.add_space(6.0);
                    ui.label(
                        egui::RichText::new("Check both devices are on the same network.")
                            .size(18.0)
                            .extra_letter_spacing(-0.3)
                            .color(ui.visuals().weak_text_color()),
                    );
                },
            );
        } else {
            let mut selected_peer = None;
            ui.add_space(20.0);
            egui::ScrollArea::horizontal()
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        for (name, endpoint_addr) in &self.peers {
                            let name = name.to_string();
                            let elapsed = self
                                .peer_pulse_started
                                .entry(name.clone())
                                .or_insert_with(Instant::now)
                                .elapsed();
                            if Self::show_peer(ui, &name, elapsed) {
                                selected_peer = Some((name, endpoint_addr.clone()));
                            }
                        }
                    });
                    ui.add_space(20.0);
                });
            if selected_peer.is_some() {
                self.selected_peer = selected_peer;
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
    fn show_peer(ui: &mut egui::Ui, name: &str, elapsed: Duration) -> bool {
        const CIRCLE_DIAMETER: f32 = 84.0;
        const TEXT_TOLERANCE: f32 = 20.0;
        const PULSE_SIZE: f32 = 20.0;
        const PULSE_DURATION: f32 = 5.0;
        const FADE_DURATION: f32 = 1.0;

        let initial = name.chars().next().unwrap_or('?').to_string();
        let text_width = CIRCLE_DIAMETER + TEXT_TOLERANCE;
        let element_width = CIRCLE_DIAMETER + PULSE_SIZE * 2.0;
        let elapsed_secs = elapsed.as_secs_f32();
        let fade = ((PULSE_DURATION - elapsed_secs) / FADE_DURATION).clamp(0.0, 1.0);

        let mut clicked = false;
        ui.vertical(|ui| {
            ui.allocate_ui(egui::vec2(element_width, 0.0), |ui| {
                ui.vertical_centered(|ui| {
                    let (rect, response) = ui.allocate_exact_size(
                        egui::vec2(element_width, CIRCLE_DIAMETER),
                        egui::Sense::click(),
                    );
                    clicked = response.clicked();

                    for (delay, pulse_size) in [(0.0, 14.0), (0.4, 17.0), (0.8, 20.0)] {
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
                        egui::FontId::proportional(24.0),
                        egui::Color32::BLACK,
                    );

                    ui.add_space(8.0);

                    ui.allocate_ui_with_layout(
                        egui::vec2(text_width, 0.0),
                        egui::Layout::top_down(egui::Align::Center),
                        |ui| {
                            ui.add(
                                egui::Label::new(
                                    egui::RichText::new(name)
                                        .color(ui.visuals().text_color().gamma_multiply(1.2)),
                                )
                                .wrap()
                                .halign(egui::Align::Center),
                            );
                        },
                    );
                });
            });
        });
        clicked
    }
    fn collect_dropped_files(&mut self, ui: &mut egui::Ui) {
        if matches!(self.send_status, Some(SendStatus::Sending)) {
            return;
        }
        let dropped_files = ui.input(|i| i.raw.dropped_files.clone());
        let Some(file) = dropped_files.into_iter().next() else {
            return;
        };
        let path = file.path().to_path_buf();
        if !path.as_os_str().is_empty() {
            self.picked_path = None;
            self.dropped_files = vec![file];
            self.send_file(path);
        }
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
            self.refresh_peers();
            self.refresh_offers();
            self.refresh_send_status();
            self.refresh_receive_status();
            ui.ctx().request_repaint_after(Duration::from_millis(250));

            self.show_header(ui);
            ui.separator();
            self.show_ticket_modal(ui);

            let content_height = ui.available_height();
            ui.allocate_ui(egui::vec2(ui.available_width(), content_height), |ui| {
                self.show_pending_offers(ui);

                if self.receive_status.is_some() {
                    self.show_receive(ui);
                } else if self.selected_peer.is_some() {
                    if self.send_status.is_some() {
                        self.show_transfer(ui);
                    } else {
                        self.show_file_picker(ui);
                    }
                } else {
                    self.show_peers(ui);
                }
            });
        });

        if self.selected_peer.is_some() {
            self.preview_files_being_dropped(ui.ctx());
            self.collect_dropped_files(ui);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_fraction_is_safe_and_clamped() {
        assert_eq!(download_fraction(0, 0), 0.0);
        assert_eq!(download_fraction(50, 100), 0.5);
        assert_eq!(download_fraction(150, 100), 1.0);
    }
}
