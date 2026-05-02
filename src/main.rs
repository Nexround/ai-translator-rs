mod api;
mod config;

use api::TranslateEvent;
use config::{Config, TARGET_LANGUAGES};
use eframe::egui;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

fn main() -> eframe::Result {
    let rt = tokio::runtime::Runtime::new().expect("failed to build tokio runtime");
    let _guard = rt.enter();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([980.0, 660.0])
            .with_title("翻译助手"),
        ..Default::default()
    };
    eframe::run_native(
        "翻译助手",
        options,
        Box::new(|cc| {
            setup_fonts(&cc.egui_ctx);
            Ok(Box::new(App::new()))
        }),
    )
}

fn setup_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    let cjk_paths = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "C:\\Windows\\Fonts\\msyh.ttc",
    ];
    for path in &cjk_paths {
        if let Ok(bytes) = std::fs::read(path) {
            fonts.font_data.insert(
                "cjk".to_owned(),
                egui::FontData::from_owned(bytes).into(),
            );
            fonts
                .families
                .entry(egui::FontFamily::Proportional)
                .or_default()
                .push("cjk".to_owned());
            fonts
                .families
                .entry(egui::FontFamily::Monospace)
                .or_default()
                .push("cjk".to_owned());
            break;
        }
    }
    ctx.set_fonts(fonts);
}

// ─── Types ────────────────────────────────────────────────────────────────────

#[derive(PartialEq)]
enum View {
    Main,
    Settings,
}

#[derive(PartialEq)]
enum TranslateState {
    Idle,
    Running,
    Testing,
}

type CancelHandle = Arc<Mutex<Option<oneshot::Sender<()>>>>;

struct App {
    config: Config,
    view: View,

    source_text: String,
    target_text: String,
    translate_state: TranslateState,
    status_msg: String,
    cancel_handle: CancelHandle,
    stream_rx: Option<Receiver<TranslateEvent>>,

    // True when the user has manually picked a target language this session;
    // suppresses auto-detection for that one translation.
    target_lang_manually_set: bool,

    settings_api_key: String,
    settings_base_url: String,
    settings_model: String,
    settings_prompt: String,
    settings_show_api_key: bool,
    settings_test_status: String,
    test_rx: Option<Receiver<Result<String, String>>>,
}

impl App {
    fn new() -> Self {
        let config = Config::load();
        App {
            settings_api_key: config.api_key.clone(),
            settings_base_url: config.base_url.clone(),
            settings_model: config.model.clone(),
            settings_prompt: config.system_prompt.clone(),
            config,
            view: View::Main,
            source_text: String::new(),
            target_text: String::new(),
            translate_state: TranslateState::Idle,
            status_msg: "就绪".to_string(),
            cancel_handle: Arc::new(Mutex::new(None)),
            stream_rx: None,
            target_lang_manually_set: false,
            settings_show_api_key: false,
            settings_test_status: String::new(),
            test_rx: None,
        }
    }

    fn start_translation(&mut self, ctx: &egui::Context) {
        let text = self.source_text.trim().to_string();
        if text.is_empty() {
            self.status_msg = "请输入要翻译的文本".to_string();
            return;
        }
        if self.config.api_key.trim().is_empty() {
            self.status_msg = "请先在设置中配置 API Key".to_string();
            return;
        }

        // Auto-detect source language: if the text is predominantly Chinese,
        // override the target language to English; otherwise use Chinese.
        if !self.target_lang_manually_set {
            self.config.target_lang = if is_predominantly_chinese(&text) {
                "英文".to_string()
            } else {
                "中文".to_string()
            };
        }
        self.target_lang_manually_set = false;

        self.config.save();
        self.target_text.clear();
        self.translate_state = TranslateState::Running;
        self.status_msg = "正在翻译...".to_string();

        let (tx, rx): (Sender<TranslateEvent>, Receiver<TranslateEvent>) = mpsc::channel();
        self.stream_rx = Some(rx);

        let api_key = self.config.api_key.clone();
        let base_url = self.config.base_url.clone();
        let model = self.config.model.clone();
        let prompt = self.config.build_prompt();
        let cancel_handle = Arc::clone(&self.cancel_handle);
        let ctx_clone = ctx.clone();

        tokio::spawn(async move {
            let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
            {
                let mut guard = cancel_handle.lock().unwrap();
                *guard = Some(cancel_tx);
            }
            let (event_tx, mut event_rx) = futures::channel::mpsc::channel(64);
            tokio::spawn(api::translate_stream(
                api_key, base_url, model, prompt, text, cancel_rx, event_tx,
            ));
            loop {
                match futures::StreamExt::next(&mut event_rx).await {
                    Some(event) => {
                        let is_terminal = matches!(
                            &event,
                            TranslateEvent::Done | TranslateEvent::Error(_)
                        );
                        let _ = tx.send(event);
                        ctx_clone.request_repaint();
                        if is_terminal {
                            break;
                        }
                    }
                    None => break,
                }
            }
        });
    }

    fn poll_stream(&mut self) {
        let rx = match self.stream_rx.take() {
            Some(r) => r,
            None => return,
        };
        let mut done = false;
        while let Ok(event) = rx.try_recv() {
            match event {
                TranslateEvent::Chunk(chunk) => self.target_text.push_str(&chunk),
                TranslateEvent::Done => {
                    self.translate_state = TranslateState::Idle;
                    self.status_msg = "翻译完成".to_string();
                    self.cancel_handle.lock().unwrap().take();
                    done = true;
                }
                TranslateEvent::Error(err) => {
                    self.translate_state = TranslateState::Idle;
                    self.status_msg = format!("错误: {}", err);
                    self.cancel_handle.lock().unwrap().take();
                    done = true;
                }
            }
        }
        if !done {
            self.stream_rx = Some(rx);
        }
    }

    fn poll_test(&mut self) {
        let rx = match self.test_rx.take() {
            Some(r) => r,
            None => return,
        };
        if let Ok(result) = rx.try_recv() {
            self.translate_state = TranslateState::Idle;
            match result {
                Ok(msg) => self.settings_test_status = format!("✓ {}", msg),
                Err(err) => self.settings_test_status = format!("✗ {}", err),
            }
        } else {
            self.test_rx = Some(rx);
        }
    }

    fn cancel_translation(&mut self) {
        if self.translate_state == TranslateState::Running {
            if let Some(tx) = self.cancel_handle.lock().unwrap().take() {
                let _ = tx.send(());
            }
            self.translate_state = TranslateState::Idle;
            self.stream_rx = None;
        }
    }
}

// ─── Colors ───────────────────────────────────────────────────────────────────

const ACCENT: egui::Color32 = egui::Color32::from_rgb(64, 133, 255);
const ACCENT_DISABLED: egui::Color32 = egui::Color32::from_rgb(160, 207, 255);
const BG_PANEL: egui::Color32 = egui::Color32::WHITE;
const BG_APP: egui::Color32 = egui::Color32::from_rgb(245, 246, 250);
const BORDER: egui::Color32 = egui::Color32::from_rgb(228, 231, 237);
const TEXT_MUTED: egui::Color32 = egui::Color32::from_rgb(144, 147, 153);
const TEXT_BODY: egui::Color32 = egui::Color32::from_rgb(48, 49, 51);

// ─── eframe::App ─────────────────────────────────────────────────────────────

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_stream();
        self.poll_test();

        if self.translate_state == TranslateState::Running
            || self.translate_state == TranslateState::Testing
        {
            ctx.request_repaint();
        }

        match self.view {
            View::Main => self.render_main(ctx),
            View::Settings => self.render_settings(ctx),
        }
    }
}

// ─── Main view ────────────────────────────────────────────────────────────────

impl App {
    fn render_main(&mut self, ctx: &egui::Context) {
        let is_translating = self.translate_state == TranslateState::Running;

        // Header
        egui::TopBottomPanel::top("header")
            .frame(
                egui::Frame::NONE
                    .fill(ACCENT)
                    .inner_margin(egui::Margin { left: 20, right: 20, top: 12, bottom: 12 }),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new("翻译助手")
                            .size(18.0)
                            .color(egui::Color32::WHITE)
                            .strong(),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new("⚙ 设置")
                                        .color(egui::Color32::WHITE)
                                        .size(13.0),
                                )
                                .frame(false),
                            )
                            .clicked()
                        {
                            self.settings_api_key = self.config.api_key.clone();
                            self.settings_base_url = self.config.base_url.clone();
                            self.settings_model = self.config.model.clone();
                            self.settings_prompt = self.config.system_prompt.clone();
                            self.settings_show_api_key = false;
                            self.settings_test_status.clear();
                            self.view = View::Settings;
                        }
                        ui.add_space(8.0);
                        let before = self.config.target_lang.clone();
                        egui::ComboBox::from_id_salt("lang_select")
                            .selected_text(&self.config.target_lang)
                            .show_ui(ui, |ui| {
                                for lang in TARGET_LANGUAGES {
                                    ui.selectable_value(
                                        &mut self.config.target_lang,
                                        lang.to_string(),
                                        *lang,
                                    );
                                }
                            });
                        if self.config.target_lang != before {
                            self.target_lang_manually_set = true;
                        }
                    });
                });
            });

        // Bottom bar
        egui::TopBottomPanel::bottom("bottom_bar")
            .frame(
                egui::Frame::NONE
                    .fill(egui::Color32::from_rgb(250, 250, 250))
                    .inner_margin(egui::Margin { left: 20, right: 20, top: 10, bottom: 10 })
                    .stroke(egui::Stroke::new(1.0, BORDER)),
            )
            .show(ctx, |ui| {
                ui.set_min_height(36.0);
                ui.horizontal(|ui| {
                    let status_color = if self.status_msg.starts_with("错误") {
                        egui::Color32::from_rgb(231, 76, 60)
                    } else if self.status_msg == "翻译完成" || self.status_msg.contains("复制") {
                        egui::Color32::from_rgb(39, 174, 96)
                    } else {
                        TEXT_MUTED
                    };
                    ui.label(
                        egui::RichText::new(&self.status_msg)
                            .size(12.0)
                            .color(status_color),
                    );

                    // Center the translate button by equal left/right spacers
                    let btn_w = 180.0;
                    let avail = ui.available_width();
                    let side = ((avail - btn_w) / 2.0).max(0.0);
                    ui.add_space(side);

                    let btn_color = if is_translating { ACCENT_DISABLED } else { ACCENT };
                    let label = if is_translating { "翻译中..." } else { "翻译  Ctrl+Enter" };
                    let make_btn = || {
                        egui::Button::new(
                            egui::RichText::new(label).color(egui::Color32::WHITE).size(14.0),
                        )
                        .fill(btn_color)
                        .min_size(egui::vec2(btn_w, 36.0))
                        .corner_radius(6.0)
                    };
                    if !is_translating && ui.add(make_btn()).clicked() {
                        self.start_translation(ctx);
                    } else if is_translating {
                        ui.add_enabled(false, make_btn());
                    }
                });
            });

        // Central area — two side-by-side panels
        egui::CentralPanel::default()
            .frame(
                egui::Frame::NONE
                    .fill(BG_APP)
                    .inner_margin(egui::Margin { left: 20, right: 20, top: 16, bottom: 16 }),
            )
            .show(ctx, |ui| {
                if ui.input(|i| i.modifiers.command && i.key_pressed(egui::Key::Enter))
                    && !is_translating
                {
                    self.start_translation(ctx);
                }

                let available = ui.available_size();
                let gap = 12.0;
                let pw = (available.x - gap) / 2.0;
                let ph = available.y;

                // Disable default item spacing so the two panels sit flush
                ui.spacing_mut().item_spacing = egui::vec2(0.0, 0.0);

                ui.horizontal(|ui| {
                    // Source panel
                    let src_rect = egui::Rect::from_min_size(ui.cursor().min, egui::vec2(pw, ph));
                    let mut src_ui = ui.new_child(
                        egui::UiBuilder::new()
                            .max_rect(src_rect)
                            .layout(egui::Layout::top_down(egui::Align::LEFT)),
                    );
                    let clear_clicked = draw_source_panel(&mut src_ui, ph, &mut self.source_text);
                    ui.advance_cursor_after_rect(src_rect);

                    if clear_clicked {
                        self.cancel_translation();
                        self.source_text.clear();
                        self.target_text.clear();
                        self.status_msg = "已清空".to_string();
                    }

                    // Gap
                    ui.add_space(gap);

                    // Target panel
                    let tgt_rect = egui::Rect::from_min_size(ui.cursor().min, egui::vec2(pw, ph));
                    let mut tgt_ui = ui.new_child(
                        egui::UiBuilder::new()
                            .max_rect(tgt_rect)
                            .layout(egui::Layout::top_down(egui::Align::LEFT)),
                    );
                    let copy_clicked =
                        draw_target_panel(&mut tgt_ui, ph, &self.target_text);
                    ui.advance_cursor_after_rect(tgt_rect);

                    if copy_clicked && !self.target_text.is_empty() {
                        if let Ok(mut cb) = arboard::Clipboard::new() {
                            let _ = cb.set_text(&self.target_text);
                            self.status_msg = "已复制到剪贴板".to_string();
                        }
                    }
                });
            });
    }
}

/// Draws the source text panel. Returns true if the clear button was clicked.
fn draw_source_panel(ui: &mut egui::Ui, height: f32, source_text: &mut String) -> bool {
    let mut clear = false;
    egui::Frame::NONE
        .fill(BG_PANEL)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .corner_radius(8.0)
        .shadow(egui::Shadow {
            color: egui::Color32::from_black_alpha(15),
            offset: [0, 2],
            blur: 8,
            spread: 0,
        })
        .inner_margin(egui::Margin::same(16))
        .show(ui, |ui| {
            ui.set_min_size(egui::vec2(ui.available_width(), height));

            // Label row
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("原文").size(12.0).color(TEXT_MUTED));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.small_button("清空").clicked() {
                        clear = true;
                    }
                });
            });
            ui.add_space(8.0);

            // Fill remaining height with text editor inside a scroll area
            let content_h = ui.available_height();
            egui::ScrollArea::vertical()
                .id_salt("source_scroll")
                .max_height(content_h)
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(source_text)
                            .hint_text("在此输入要翻译的文本...")
                            .desired_rows(30)
                            .font(egui::TextStyle::Body)
                            .desired_width(f32::INFINITY),
                    );
                });
        });
    clear
}

/// Draws the target text panel. Returns true if the copy button was clicked.
fn draw_target_panel(ui: &mut egui::Ui, height: f32, target_text: &str) -> bool {
    let mut copy = false;
    egui::Frame::NONE
        .fill(BG_PANEL)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .corner_radius(8.0)
        .shadow(egui::Shadow {
            color: egui::Color32::from_black_alpha(15),
            offset: [0, 2],
            blur: 8,
            spread: 0,
        })
        .inner_margin(egui::Margin::same(16))
        .show(ui, |ui| {
            ui.set_min_size(egui::vec2(ui.available_width(), height));

            // Label row
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("译文").size(12.0).color(TEXT_MUTED));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.small_button("复制").clicked() {
                        copy = true;
                    }
                });
            });
            ui.add_space(8.0);

            // Fill remaining height with scrollable text
            let content_h = ui.available_height();
            egui::ScrollArea::vertical()
                .id_salt("target_scroll")
                .max_height(content_h)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    if target_text.is_empty() {
                        ui.label(
                            egui::RichText::new("翻译结果将在这里显示...")
                                .size(14.0)
                                .color(egui::Color32::from_rgb(192, 196, 204)),
                        );
                    } else {
                        ui.label(
                            egui::RichText::new(target_text).size(14.0).color(TEXT_BODY),
                        );
                    }
                });
        });
    copy
}

// ─── Settings view ────────────────────────────────────────────────────────────

impl App {
    fn render_settings(&mut self, ctx: &egui::Context) {
        let is_testing = self.translate_state == TranslateState::Testing;

        egui::TopBottomPanel::top("settings_header")
            .frame(
                egui::Frame::NONE
                    .fill(ACCENT)
                    .inner_margin(egui::Margin { left: 20, right: 20, top: 12, bottom: 12 }),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    if ui
                        .add(
                            egui::Button::new(
                                egui::RichText::new("← 返回")
                                    .color(egui::Color32::WHITE)
                                    .size(14.0),
                            )
                            .frame(false),
                        )
                        .clicked()
                    {
                        self.view = View::Main;
                    }
                    ui.add_space(8.0);
                    ui.label(
                        egui::RichText::new("设置")
                            .size(18.0)
                            .color(egui::Color32::WHITE)
                            .strong(),
                    );
                });
            });

        egui::CentralPanel::default()
            .frame(
                egui::Frame::NONE
                    .fill(BG_APP)
                    .inner_margin(egui::Margin { left: 40, right: 40, top: 24, bottom: 24 }),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    let max_w = ui.available_width().min(680.0);
                    ui.set_max_width(max_w);

                    // API card
                    settings_card(ui, |ui| {
                        ui.label(
                            egui::RichText::new("API 配置")
                                .size(15.0)
                                .color(TEXT_BODY)
                                .strong(),
                        );
                        ui.add_space(16.0);

                        field_label(ui, "API Key");
                        ui.add_space(4.0);
                        ui.horizontal(|ui| {
                            let avail = ui.available_width();
                            let checkbox_w = 70.0;
                            ui.add(
                                egui::TextEdit::singleline(&mut self.settings_api_key)
                                    .hint_text("sk-...")
                                    .password(!self.settings_show_api_key)
                                    .desired_width(avail - checkbox_w),
                            );
                            ui.checkbox(&mut self.settings_show_api_key, "显示");
                        });
                        ui.add_space(14.0);

                        field_label(ui, "Base URL");
                        ui.add_space(4.0);
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings_base_url)
                                .hint_text("https://api.openai.com/v1")
                                .desired_width(f32::INFINITY),
                        );
                        ui.add_space(14.0);

                        field_label(ui, "模型");
                        ui.add_space(4.0);
                        ui.add(
                            egui::TextEdit::singleline(&mut self.settings_model)
                                .hint_text("gpt-4o-mini")
                                .desired_width(f32::INFINITY),
                        );
                        ui.add_space(16.0);

                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(
                                    !is_testing,
                                    egui::Button::new(if is_testing { "测试中..." } else { "测试连接" }),
                                )
                                .clicked()
                            {
                                let api_key = self.settings_api_key.trim().to_string();
                                if api_key.is_empty() {
                                    self.settings_test_status = "请先输入 API Key".to_string();
                                } else {
                                    let base_url = non_empty_or(
                                        &self.settings_base_url,
                                        "https://api.openai.com/v1",
                                    );
                                    let model = non_empty_or(&self.settings_model, "gpt-4o-mini");
                                    self.translate_state = TranslateState::Testing;
                                    self.settings_test_status = "测试中...".to_string();

                                    let (tx, rx) = mpsc::channel();
                                    self.test_rx = Some(rx);
                                    let ctx_clone = ctx.clone();
                                    tokio::spawn(async move {
                                        let result =
                                            api::test_connection(api_key, base_url, model).await;
                                        let _ = tx.send(result);
                                        ctx_clone.request_repaint();
                                    });
                                }
                            }
                            ui.add_space(12.0);
                            let test_color = if self.settings_test_status.starts_with('✓') {
                                egui::Color32::from_rgb(39, 174, 96)
                            } else if self.settings_test_status.starts_with('✗') {
                                egui::Color32::from_rgb(231, 76, 60)
                            } else {
                                TEXT_MUTED
                            };
                            ui.label(
                                egui::RichText::new(&self.settings_test_status)
                                    .size(12.0)
                                    .color(test_color),
                            );
                        });
                    });

                    ui.add_space(16.0);

                    // Prompt card
                    settings_card(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(
                                egui::RichText::new("系统提示词")
                                    .size(15.0)
                                    .color(TEXT_BODY)
                                    .strong(),
                            );
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if ui.small_button("恢复默认").clicked() {
                                    self.settings_prompt =
                                        config::DEFAULT_SYSTEM_PROMPT.to_string();
                                }
                            });
                        });
                        ui.add_space(6.0);
                        ui.label(
                            egui::RichText::new("{target_lang} 将替换为所选目标语言")
                                .size(11.0)
                                .color(TEXT_MUTED),
                        );
                        ui.add_space(10.0);
                        ui.add(
                            egui::TextEdit::multiline(&mut self.settings_prompt)
                                .desired_rows(6)
                                .desired_width(f32::INFINITY),
                        );
                    });

                    ui.add_space(20.0);

                    // Action buttons — right-aligned
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new("保存").color(egui::Color32::WHITE),
                                )
                                .fill(ACCENT)
                                .min_size(egui::vec2(80.0, 34.0))
                                .corner_radius(6.0),
                            )
                            .clicked()
                        {
                            self.config.api_key = self.settings_api_key.trim().to_string();
                            self.config.base_url =
                                non_empty_or(&self.settings_base_url, "https://api.openai.com/v1");
                            self.config.model = non_empty_or(&self.settings_model, "gpt-4o-mini");
                            self.config.system_prompt = if self.settings_prompt.trim().is_empty() {
                                config::DEFAULT_SYSTEM_PROMPT.to_string()
                            } else {
                                self.settings_prompt.trim().to_string()
                            };
                            self.config.save();
                            self.view = View::Main;
                        }
                        ui.add_space(10.0);
                        if ui
                            .add(
                                egui::Button::new("取消")
                                    .min_size(egui::vec2(80.0, 34.0))
                                    .corner_radius(6.0),
                            )
                            .clicked()
                        {
                            self.view = View::Main;
                        }
                    });
                });
            });
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn settings_card(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui)) {
    egui::Frame::NONE
        .fill(BG_PANEL)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .corner_radius(10.0)
        .shadow(egui::Shadow {
            color: egui::Color32::from_black_alpha(13),
            offset: [0, 2],
            blur: 6,
            spread: 0,
        })
        .inner_margin(egui::Margin::same(20))
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            add_contents(ui);
        });
}

fn field_label(ui: &mut egui::Ui, text: &str) {
    ui.label(
        egui::RichText::new(text)
            .size(12.0)
            .color(egui::Color32::from_rgb(96, 98, 102)),
    );
}

fn non_empty_or(s: &str, fallback: &str) -> String {
    let t = s.trim();
    if t.is_empty() { fallback.to_string() } else { t.to_string() }
}

/// Returns true when more than 20% of the letters in `text` are CJK characters,
/// which we treat as "predominantly Chinese" (also covers Japanese/Korean kanji
/// but that's an acceptable heuristic for this use case).
fn is_predominantly_chinese(text: &str) -> bool {
    let mut total = 0usize;
    let mut cjk = 0usize;
    for ch in text.chars() {
        if ch.is_alphabetic() || matches!(ch as u32,
            0x4E00..=0x9FFF   // CJK Unified Ideographs
            | 0x3400..=0x4DBF  // CJK Extension A
            | 0x20000..=0x2A6DF // CJK Extension B
            | 0xF900..=0xFAFF  // CJK Compatibility Ideographs
            | 0x3000..=0x303F  // CJK Symbols and Punctuation
            | 0xFF00..=0xFFEF  // Halfwidth/Fullwidth Forms
        ) {
            total += 1;
            if matches!(ch as u32,
                0x4E00..=0x9FFF
                | 0x3400..=0x4DBF
                | 0x20000..=0x2A6DF
                | 0xF900..=0xFAFF
            ) {
                cjk += 1;
            }
        }
    }
    total > 0 && cjk * 5 > total // >20% CJK
}
