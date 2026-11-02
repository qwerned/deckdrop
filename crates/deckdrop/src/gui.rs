//! Окно приложения. Вся логика в службе, здесь только показ и команды.
use crate::ctl::Client;
use eframe::egui::{
    self, Align, Align2, Color32, CornerRadius, FontData, FontDefinitions, FontFamily, FontId, Frame, Layout, Margin, Pos2, Rect, RichText, Sense, Stroke, Ui,
    Vec2,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const GROUND: Color32 = Color32::from_rgb(0x11, 0x13, 0x16);
const PANEL: Color32 = Color32::from_rgb(0x1A, 0x1D, 0x21);
const RAISED: Color32 = Color32::from_rgb(0x23, 0x27, 0x2C);
const LINE: Color32 = Color32::from_rgb(0x2E, 0x33, 0x38);
const TEXT: Color32 = Color32::from_rgb(0xEC, 0xEF, 0xF1);
const MUTED: Color32 = Color32::from_rgb(0xA7, 0xAF, 0xB7);
const OK: Color32 = Color32::from_rgb(0x6E, 0xE7, 0xB7);
const BAD: Color32 = Color32::from_rgb(0xFF, 0x8A, 0x7A);

fn accent() -> Color32 {
    if cfg!(target_os = "linux") {
        Color32::from_rgb(0x1A, 0x9F, 0xFF) // синий Steam
    } else {
        Color32::from_rgb(0xFF, 0xB2, 0x24)
    }
}

fn on_accent() -> Color32 {
    if cfg!(target_os = "linux") {
        Color32::from_rgb(0x06, 0x12, 0x1C)
    } else {
        Color32::from_rgb(0x16, 0x13, 0x0A)
    }
}

fn semibold(sz: f32) -> FontId {
    FontId::new(sz, FontFamily::Name("semibold".into()))
}

fn sans(sz: f32) -> FontId {
    FontId::new(sz, FontFamily::Proportional)
}

fn mono(sz: f32) -> FontId {
    FontId::new(sz, FontFamily::Monospace)
}

fn mono_med(sz: f32) -> FontId {
    FontId::new(sz, FontFamily::Name("mono-medium".into()))
}

fn comma(x: f64, d: usize) -> String {
    format!("{x:.d$}").replace('.', ",")
}

fn size(b: f64) -> String {
    if b >= 1e9 {
        format!("{} ГБ", comma(b / 1e9, 1))
    } else if b >= 1e6 {
        format!("{} МБ", comma(b / 1e6, 0))
    } else if b >= 1e3 {
        format!("{} КБ", comma(b / 1e3, 0))
    } else {
        format!("{} Б", b as u64)
    }
}

fn spaced(n: u64) -> String {
    let s = n.to_string();
    let mut o = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            o.push('\u{a0}');
        }
        o.push(c);
    }
    o
}

fn plural(n: u64, one: &str, few: &str, many: &str) -> String {
    let (a, b) = (n % 10, n % 100);
    let w = if a == 1 && b != 11 {
        one
    } else if (2..=4).contains(&a) && !(12..=14).contains(&b) {
        few
    } else {
        many
    };
    format!("{} {w}", spaced(n))
}

fn duration(s: f64) -> String {
    let s = s.max(0.0) as u64;
    if s >= 3600 {
        format!("{} ч {} мин", s / 3600, s % 3600 / 60)
    } else if s >= 60 {
        format!("{} мин {:02} с", s / 60, s % 60)
    } else {
        format!("{s} с")
    }
}

/// Кнопка с полями и текстом по центру: стандартная прижимает текст к краю при заданной ширине.
fn plain_button(ui: &mut Ui, text: &str, font: FontId, fill: Color32, fg: Color32, min_w: f32, h: f32, radius: u8) -> egui::Response {
    let galley = ui.painter().layout_no_wrap(text.to_string(), font, fg);
    let w = min_w.max(galley.size().x + 28.0);
    let (rect, resp) = ui.allocate_exact_size(Vec2::new(w, h), Sense::click());
    let enabled = ui.is_enabled();
    let k = if !enabled {
        0.45
    } else if resp.is_pointer_button_down_on() {
        0.85
    } else {
        1.0
    };
    let fill = if resp.hovered() && enabled && fill != Color32::TRANSPARENT { fill.lerp_to_gamma(TEXT, 0.06) } else { fill };
    ui.painter().rect_filled(rect, CornerRadius::same(radius), fill.gamma_multiply(k));
    let pos = rect.center() - galley.size() / 2.0;
    ui.painter().galley(pos, galley, fg.gamma_multiply(k));
    resp.on_hover_cursor(egui::CursorIcon::PointingHand)
}

fn button(ui: &mut Ui, text: &str, primary: bool, w: f32, h: f32) -> egui::Response {
    let (fill, fg) = if primary { (accent(), on_accent()) } else { (RAISED, TEXT) };
    plain_button(ui, text, semibold(15.0), fill, fg, w, h, 12)
}

fn chip(ui: &mut Ui, text: &str, on: bool) -> egui::Response {
    let (fill, fg) = if on { (TEXT, GROUND) } else { (PANEL, MUTED) };
    plain_button(ui, text, sans(13.0), fill, fg, 0.0, 30.0, 15)
}

/// Сегмент-переключатель на всю ширину. Возвращает нажатый индекс.
fn segment(ui: &mut Ui, items: &[&str], selected: usize) -> Option<usize> {
    let w = ui.available_width();
    let (outer, _) = ui.allocate_exact_size(Vec2::new(w, 48.0), Sense::hover());
    ui.painter().rect_filled(outer, CornerRadius::same(10), PANEL);
    let cell = (w - 8.0) / items.len() as f32;
    let mut hit = None;
    for (i, t) in items.iter().enumerate() {
        let r = Rect::from_min_size(outer.min + Vec2::new(4.0 + cell * i as f32, 4.0), Vec2::new(cell, 40.0));
        let on = i == selected;
        let resp = ui.interact(r, ui.id().with(("segment", i)), Sense::click()).on_hover_cursor(egui::CursorIcon::PointingHand);
        if on {
            ui.painter().rect_filled(r, CornerRadius::same(7), LINE);
        }
        let (font, col) = if on { (semibold(14.0), TEXT) } else { (sans(14.0), MUTED) };
        ui.painter().text(r.center(), Align2::CENTER_CENTER, *t, font, col);
        if resp.clicked() {
            hit = Some(i);
        }
    }
    hit
}

/// Нижняя панель заданной высоты, закреплённая у края области.
fn footer(ui: &mut Ui, h: f32, add: impl FnOnce(&mut Ui)) {
    let r = ui.max_rect();
    let rect = Rect::from_min_max(Pos2::new(r.left(), r.bottom() - h), r.right_bottom());
    ui.scope_builder(egui::UiBuilder::new().max_rect(rect).layout(Layout::left_to_right(Align::Center)), add);
}

fn card(ui: &mut Ui, fill: Color32, add: impl FnOnce(&mut Ui)) {
    Frame::new().fill(fill).corner_radius(CornerRadius::same(12)).inner_margin(Margin::symmetric(14, 11)).show(ui, |ui| {
        ui.set_width(ui.available_width());
        add(ui)
    });
}

fn bar(ui: &mut Ui, frac: f32, h: f32, color: Color32) {
    let (r, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), h), Sense::hover());
    let p = ui.painter();
    let rad = CornerRadius::same((h / 2.0) as u8);
    p.rect_filled(r, rad, LINE);
    let w = r.width() * frac.clamp(0.0, 1.0);
    if w > 0.5 {
        p.rect_filled(Rect::from_min_size(r.min, Vec2::new(w.max(h), h)), rad, color);
    }
}

fn graph(ui: &mut Ui, samples: &[f64], avg: f64, h: f32) {
    let (r, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), h), Sense::hover());
    let p = ui.painter_at(r);
    p.rect_filled(r, CornerRadius::same(10), PANEL);
    let max = samples.iter().cloned().fold(avg.max(50e6), f64::max) * 1.15;
    let y = |v: f64| r.bottom() - 4.0 - (v / max) as f32 * (h - 8.0);
    let n = 120usize;
    let x = |i: usize| r.left() + i as f32 / (n - 1) as f32 * r.width();
    let start = n.saturating_sub(samples.len());
    let pts: Vec<Pos2> = samples.iter().rev().take(n).rev().enumerate().map(|(i, v)| Pos2::new(x(start + i), y(*v))).collect();
    let fill = accent().gamma_multiply(0.14);
    for w in pts.windows(2) {
        let rect = Rect::from_min_max(Pos2::new(w[0].x, w[1].y.min(w[0].y)), Pos2::new(w[1].x, r.bottom() - 1.0));
        p.rect_filled(rect, CornerRadius::ZERO, fill);
    }
    if avg > 0.0 {
        let yy = y(avg);
        let dash: Vec<egui::Shape> = egui::Shape::dashed_line(&[Pos2::new(r.left(), yy), Pos2::new(r.right(), yy)], Stroke::new(1.0_f32, MUTED), 3.0, 5.0);
        p.extend(dash);
    }
    if pts.len() > 1 {
        p.line(pts, Stroke::new(2.0_f32, accent()));
    }
}

#[derive(Default)]
struct Shared {
    status: Option<Value>,
    files: Value,
    error: String,
    online: bool,
}

pub struct App {
    shared: Arc<Mutex<Shared>>,
    cmd: Sender<Value>,
    files_req: Arc<Mutex<(String, String)>>,
    receive_tab: bool,
    picks: Vec<PathBuf>,
    device: Option<String>,
    dest: Option<String>,
    filter: String,
    dismissed: Vec<String>,
    code_input: String,
    excluded: Vec<String>,
    offer_dest: Option<String>,
    adding: bool,
}

fn load_fonts(ctx: &egui::Context) {
    let mut f = FontDefinitions::default();
    let mut add = |n: &str, d: &'static [u8]| {
        f.font_data.insert(n.into(), Arc::new(FontData::from_static(d)));
    };
    add("sans", include_bytes!("../assets/IBMPlexSans-Regular.ttf"));
    add("sans-sb", include_bytes!("../assets/IBMPlexSans-SemiBold.ttf"));
    add("mono", include_bytes!("../assets/IBMPlexMono-Regular.ttf"));
    add("mono-md", include_bytes!("../assets/IBMPlexMono-Medium.ttf"));
    f.families.insert(FontFamily::Proportional, vec!["sans".into()]);
    f.families.insert(FontFamily::Monospace, vec!["mono".into()]);
    f.families.insert(FontFamily::Name("semibold".into()), vec!["sans-sb".into()]);
    f.families.insert(FontFamily::Name("mono-medium".into()), vec!["mono-md".into()]);
    ctx.set_fonts(f);
}

impl App {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        load_fonts(&cc.egui_ctx);
        let mut v = egui::Visuals::dark();
        v.panel_fill = GROUND;
        v.window_fill = PANEL;
        v.override_text_color = Some(TEXT);
        v.widgets.inactive.weak_bg_fill = RAISED;
        v.widgets.inactive.bg_stroke = Stroke::NONE;
        v.widgets.hovered.weak_bg_fill = LINE;
        v.widgets.hovered.bg_stroke = Stroke::NONE;
        v.widgets.active.weak_bg_fill = LINE;
        v.selection.bg_fill = accent().gamma_multiply(0.5);
        cc.egui_ctx.set_visuals(v);
        if cfg!(target_os = "linux") {
            cc.egui_ctx.set_zoom_factor(1.2); // на Deck маленький экран и пальцы
        }
        let shared = Arc::new(Mutex::new(Shared::default()));
        let (tx, rx) = channel::<Value>();
        let files_req = Arc::new(Mutex::new((String::new(), "all".to_string())));
        {
            let (shared, ctx, fr) = (shared.clone(), cc.egui_ctx.clone(), files_req.clone());
            std::thread::spawn(move || {
                let mut c: Option<Client> = None;
                loop {
                    if c.is_none() {
                        c = Client::connect_or_spawn().ok();
                    }
                    let Some(cl) = c.as_mut() else {
                        shared.lock().unwrap().online = false;
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    };
                    while let Ok(m) = rx.try_recv() {
                        if let Err(e) = cl.call(m) {
                            shared.lock().unwrap().error = e;
                        }
                    }
                    match cl.call(json!({"cmd": "status"})) {
                        Ok(s) => {
                            let mut g = shared.lock().unwrap();
                            g.status = Some(s);
                            g.online = true;
                        }
                        Err(_) => {
                            c = None;
                            continue;
                        }
                    }
                    let (job, filter) = fr.lock().unwrap().clone();
                    if !job.is_empty() {
                        if let Ok(v) = cl.call(json!({"cmd": "files", "job": job, "filter": filter, "limit": 200})) {
                            shared.lock().unwrap().files = v;
                        }
                    }
                    ctx.request_repaint();
                    std::thread::sleep(Duration::from_millis(400));
                }
            });
        }
        Self {
            shared,
            cmd: tx,
            files_req,
            receive_tab: false,
            picks: vec![],
            device: None,
            dest: None,
            filter: "all".into(),
            dismissed: vec![],
            code_input: String::new(),
            excluded: vec![],
            offer_dest: None,
            adding: false,
        }
    }

    fn send(&self, v: Value) {
        let _ = self.cmd.send(v);
    }

    fn header_idle(&self, ui: &mut Ui, st: &Value) {
        ui.horizontal(|ui| {
            ui.set_height(44.0);
            ui.label(RichText::new("DeckDrop").font(semibold(16.0)));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let name = st["me"]["name"].as_str().unwrap_or("");
                let free = st["me"]["roots"][0]["free"].as_f64().unwrap_or(0.0);
                ui.label(RichText::new(format!("{name} готов принимать · свободно {}", size(free))).font(sans(13.0)).color(MUTED));
            });
        });
        ui.separator();
    }

    fn devices_panel(&mut self, ui: &mut Ui, st: &Value) {
        let devices = st["devices"].as_array().cloned().unwrap_or_default();
        for d in devices.iter().filter(|d| d["paired"] == true) {
            let id = d["id"].as_str().unwrap_or("").to_string();
            if self.device.is_none() {
                self.device = Some(id.clone());
            }
            let sel = self.device.as_deref() == Some(&id);
            let fill = if sel { RAISED } else { PANEL };
            let online = d["online"] == true;
            let resp = Frame::new().fill(fill).corner_radius(CornerRadius::same(12)).inner_margin(Margin::same(14)).show(ui, |ui| {
                ui.set_width(ui.available_width());
                ui.label(RichText::new(d["name"].as_str().unwrap_or("")).font(semibold(14.0)));
                let line = if online {
                    format!("рядом · {}", d["addr"].as_str().unwrap_or("").split(':').next().unwrap_or(""))
                } else {
                    "не в сети".to_string()
                };
                ui.label(RichText::new(line).font(sans(12.0)).color(MUTED));
                if let Some(r) = d["roots"].as_array().and_then(|r| r.first()) {
                    ui.label(RichText::new(format!("свободно {}", size(r["free"].as_f64().unwrap_or(0.0)))).font(sans(12.0)).color(MUTED));
                }
            });
            if resp.response.interact(Sense::click()).clicked() {
                self.device = Some(id);
                self.dest = None;
            }
            ui.add_space(6.0);
        }
        if plain_button(ui, "Добавить устройство", sans(14.0), PANEL, MUTED, ui.available_width(), 48.0, 12).clicked() {
            self.adding = true;
            self.send(json!({"cmd": "scan"}));
        }
    }

    fn send_tab(&mut self, ui: &mut Ui, st: &Value, has_job: bool) {
        let device = st["devices"].as_array().and_then(|a| a.iter().find(|d| d["id"].as_str() == self.device.as_deref())).cloned();
        let roots = device.as_ref().and_then(|d| d["roots"].as_array().cloned()).unwrap_or_default();
        if self.dest.is_none() {
            self.dest = roots.first().and_then(|r| r["path"].as_str().map(String::from));
        }
        let online = device.as_ref().is_some_and(|d| d["online"] == true);
        // Если к устройству уже идёт отправка, новая встанет за ней.
        let busy =
            st["jobs"].as_array().is_some_and(|a| a.iter().any(|j| j["role"] == "send" && j["peer"].as_str() == self.device.as_deref() && !is_finished(j)));
        ui.horizontal(|ui| {
            if button(ui, "Файлы", false, 80.0, 40.0).clicked() {
                if let Some(v) = rfd::FileDialog::new().pick_files() {
                    self.picks.extend(v);
                }
            }
            if button(ui, "Папка", false, 80.0, 40.0).clicked() {
                if let Some(v) = rfd::FileDialog::new().pick_folders() {
                    self.picks.extend(v);
                }
            }
            egui::ComboBox::from_id_salt("dest")
                .selected_text(RichText::new(format!("в {}", self.dest.clone().unwrap_or_else(|| "папку по умолчанию".into()))).font(mono(13.0)))
                .height(240.0)
                .show_ui(ui, |ui| {
                    for r in &roots {
                        let p = r["path"].as_str().unwrap_or("").to_string();
                        let label = format!("{p} · свободно {}", size(r["free"].as_f64().unwrap_or(0.0)));
                        ui.selectable_value(&mut self.dest, Some(p), label);
                    }
                });
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                ui.add_enabled_ui(online && !self.picks.is_empty(), |ui| {
                    if button(ui, if busy { "В очередь" } else { "Отправить" }, true, 130.0, 40.0).clicked() {
                        let paths: Vec<String> = self.picks.iter().map(|p| p.to_string_lossy().into_owned()).collect();
                        self.send(json!({"cmd": "send", "peer": self.device, "paths": paths, "dest": self.dest}));
                        self.picks.clear();
                    }
                });
            });
        });
        ui.add_space(6.0);
        ui.ctx().input(|i| {
            for f in &i.raw.dropped_files {
                if let Some(p) = &f.path {
                    self.picks.push(p.clone());
                }
            }
        });
        self.picks.dedup();
        let mut remove = None;
        let max_h = if has_job { 68.0 } else { 204.0 };
        egui::ScrollArea::vertical().id_salt("picks").max_height(max_h).auto_shrink([false, true]).show(ui, |ui| {
            for (i, p) in self.picks.iter().enumerate() {
                ui.horizontal(|ui| {
                    ui.set_height(30.0);
                    let w = ui.available_width();
                    ui.allocate_ui_with_layout(Vec2::new(w - 44.0, 26.0), Layout::left_to_right(Align::Center), |ui| {
                        ui.set_width(w - 44.0);
                        ui.label(RichText::new(p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()).font(semibold(14.0)));
                        ui.add(egui::Label::new(RichText::new(p.display().to_string()).font(sans(12.0)).color(MUTED)).truncate());
                    });
                    if ui.add(egui::Button::new("×").fill(PANEL).min_size(Vec2::splat(30.0))).on_hover_text("Убрать").clicked() {
                        remove = Some(i);
                    }
                });
            }
        });
        if let Some(i) = remove {
            self.picks.remove(i);
        }
        if !self.picks.is_empty() {
            ui.add_space(6.0);
        }
    }

    fn drop_zone(&self, ui: &mut Ui, reserve: f32) {
        let (r, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), (ui.available_height() - reserve).max(40.0)), Sense::hover());
        ui.painter().rect_stroke(r, CornerRadius::same(12), Stroke::new(1.5_f32, LINE), egui::StrokeKind::Inside);
        ui.painter().text(r.center(), Align2::CENTER_CENTER, "Перетащите файлы и папки сюда", sans(14.0), MUTED);
    }

    fn receive_tab_ui_idle(&self, ui: &mut Ui, reserve: f32) {
        let (r, _) = ui.allocate_exact_size(Vec2::new(ui.available_width(), (ui.available_height() - reserve).max(40.0)), Sense::hover());
        ui.painter().text(r.center() - Vec2::new(0.0, 14.0), Align2::CENTER_CENTER, "Жду файлы", semibold(22.0), TEXT);
        ui.painter().text(
            r.center() + Vec2::new(0.0, 14.0),
            Align2::CENTER_CENTER,
            "Отправьте их с другого устройства, запрос появится здесь",
            sans(14.0),
            MUTED,
        );
    }

    fn receive_tab_ui(&mut self, ui: &mut Ui, st: &Value) {
        let offers = st["offers"].as_array().cloned().unwrap_or_default();
        let Some(o) = offers.first() else { return };
        let oid = o["offer"].as_str().unwrap_or("").to_string();
        ui.label(RichText::new(format!("{} отправляет файлы", o["peer_name"].as_str().unwrap_or(""))).font(semibold(22.0)));
        if o["from_llm"] == true {
            ui.label(RichText::new("Запрос создан LLM").color(accent()));
        }
        if o["in_place"] == true {
            ui.label(RichText::new("Обновление на месте: файлы в папке будут заменены. Если передача прервётся, она продолжится.").color(accent()));
        }
        ui.label(RichText::new("Снимите отметку с того, что не нужно").color(MUTED));
        ui.add_space(8.0);
        let mut total = 0f64;
        let items = o["items"].as_array().cloned().unwrap_or_default();
        for it in &items {
            let name = it["name"].as_str().unwrap_or("").to_string();
            let mut on = !self.excluded.contains(&name);
            card(ui, PANEL, |ui| {
                ui.horizontal(|ui| {
                    if ui.checkbox(&mut on, "").changed() {
                        if on {
                            self.excluded.retain(|x| x != &name);
                        } else {
                            self.excluded.push(name.clone());
                        }
                    }
                    ui.vertical(|ui| {
                        ui.label(RichText::new(&name).font(semibold(14.0)));
                        ui.label(RichText::new(plural(it["files"].as_u64().unwrap_or(0), "файл", "файла", "файлов")).font(sans(12.0)).color(MUTED));
                    });
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.label(RichText::new(size(it["size"].as_f64().unwrap_or(0.0))).font(mono(13.0)));
                    });
                });
            });
            if on {
                total += it["size"].as_f64().unwrap_or(0.0);
            }
            ui.add_space(6.0);
        }
        let conflict: Vec<String> = o["conflict"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default();
        if !conflict.is_empty() && o["overwrite"] != true {
            ui.label(RichText::new(format!("Уже есть в папке назначения: {}", conflict.join(", "))).color(BAD));
        }
        {
            footer(ui, 64.0, |ui| {
                let dest = self.offer_dest.clone().unwrap_or_else(|| o["dest"].as_str().unwrap_or("").to_string());
                if button(ui, &format!("в {dest}"), false, 120.0, 48.0).clicked() {
                    if let Some(p) = rfd::FileDialog::new().pick_folder() {
                        self.offer_dest = Some(p.to_string_lossy().into_owned());
                    }
                }
                let free = st["me"]["roots"][0]["free"].as_f64().unwrap_or(0.0);
                ui.label(RichText::new(format!("останется {}", size((free - total).max(0.0)))).font(sans(13.0)).color(MUTED));
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if button(ui, &format!("Принять · {}", size(total)), true, 170.0, 52.0).clicked() {
                        self.send(json!({"cmd": "accept", "offer": oid, "dest": dest, "exclude": self.excluded}));
                        self.excluded.clear();
                        self.offer_dest = None;
                    }
                    if button(ui, "Отклонить", false, 100.0, 52.0).clicked() {
                        self.send(json!({"cmd": "decline", "offer": oid}));
                    }
                });
            });
        }
    }

    fn pair_ui(&mut self, ui: &mut Ui, st: &Value) {
        let pair = &st["pair"];
        ui.add_space(24.0);
        ui.vertical_centered(|ui| {
            ui.set_max_width(520.0);
            match pair["status"].as_str() {
                Some("show_code") => {
                    ui.label(RichText::new(format!("Нашёлся {}", pair["peer_name"].as_str().unwrap_or(""))).font(semibold(24.0)));
                    ui.label(RichText::new("Введите этот код на другом устройстве. Это нужно один раз.").color(MUTED));
                    ui.add_space(20.0);
                    let c = pair["code"].as_str().unwrap_or("");
                    let pretty = if c.len() == 6 { format!("{} {}", &c[..3], &c[3..]) } else { c.to_string() };
                    ui.label(RichText::new(pretty).font(mono_med(64.0)).color(accent()));
                    ui.label(RichText::new(format!("Код действует ещё {} с", pair["secs_left"].as_u64().unwrap_or(0))).color(MUTED));
                }
                Some("enter_code") | Some("checking") | Some("bad_code") => {
                    ui.label(RichText::new(format!("{} хочет подключиться", pair["peer_name"].as_str().unwrap_or(""))).font(semibold(22.0)));
                    ui.label(RichText::new("Введите код с его экрана").color(MUTED));
                    ui.add_space(8.0);
                    let d = self.code_input.clone().chars().chain(std::iter::repeat('_')).take(6).collect::<String>();
                    ui.label(RichText::new(format!("{} {}", &d[..3], &d[3..])).font(mono_med(44.0)));
                    if pair["status"] == "bad_code" {
                        ui.label(RichText::new("Код не подошёл. Начните сопряжение заново.").color(BAD));
                    }
                    ui.add_space(8.0);
                    let keys = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "×", "0", "←"];
                    egui::Grid::new("pad").spacing(Vec2::splat(8.0)).show(ui, |ui| {
                        for (i, k) in keys.iter().enumerate() {
                            let r = ui.add(
                                egui::Button::new(RichText::new(*k).font(mono(24.0)))
                                    .fill(RAISED)
                                    .stroke(Stroke::NONE)
                                    .corner_radius(CornerRadius::same(12))
                                    .min_size(Vec2::new(84.0, 56.0)),
                            );
                            if r.clicked() {
                                match *k {
                                    "×" => self.code_input.clear(),
                                    "←" => {
                                        self.code_input.pop();
                                    }
                                    n if self.code_input.len() < 6 => {
                                        self.code_input.push_str(n);
                                        if self.code_input.len() == 6 {
                                            self.send(json!({"cmd": "pair_enter", "code": self.code_input}));
                                            self.code_input.clear();
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            if i % 3 == 2 {
                                ui.end_row();
                            }
                        }
                    });
                }
                Some("paired") => {
                    ui.label(RichText::new("Готово, устройства сопряжены").font(semibold(24.0)).color(OK));
                    ui.add_space(12.0);
                    if button(ui, "Хорошо", true, 140.0, 48.0).clicked() {
                        self.adding = false;
                    }
                }
                _ => {
                    ui.label(RichText::new("Добавить устройство").font(semibold(22.0)));
                    ui.label(RichText::new("Запустите DeckDrop на другом устройстве в этой же сети").color(MUTED));
                    ui.add_space(12.0);
                    let unpaired: Vec<Value> = st["devices"].as_array().cloned().unwrap_or_default().into_iter().filter(|d| d["paired"] == false).collect();
                    if unpaired.is_empty() {
                        ui.label(RichText::new("Ищу устройства…").color(MUTED));
                    }
                    for d in unpaired {
                        if button(ui, d["name"].as_str().unwrap_or(""), false, 300.0, 52.0).clicked() {
                            self.send(json!({"cmd": "pair_start", "peer": d["id"]}));
                        }
                    }
                    ui.add_space(12.0);
                    if button(ui, "Закрыть", false, 120.0, 44.0).clicked() {
                        self.adding = false;
                    }
                }
            }
        });
    }

    /// Текущая передача: живёт на главном экране, ниже выбора файлов.
    fn transfer_panel(&mut self, ui: &mut Ui, j: &Value, reserve: f32) {
        let id = j["id"].as_str().unwrap_or("").to_string();
        *self.files_req.lock().unwrap() = (id.clone(), self.filter.clone());
        let state = j["state"].as_str().unwrap_or("");
        let phase = j["phase"].as_str().unwrap_or("");
        let paused = j["paused"] == true;
        let lost = matches!(state, "reconnecting" | "interrupted");
        let (bytes, total) = (j["bytes"].as_f64().unwrap_or(0.0), j["total"].as_f64().unwrap_or(0.0));
        let speed = j["speed"].as_f64().unwrap_or(0.0);
        let avg = j["avg_speed"].as_f64().unwrap_or(0.0);

        ui.horizontal(|ui| {
            ui.set_height(38.0);
            let w = ui.available_width();
            ui.allocate_ui_with_layout(Vec2::new((w - 230.0).max(60.0), 36.0), Layout::left_to_right(Align::Center), |ui| {
                ui.set_width((w - 230.0).max(60.0));
                ui.label(RichText::new(j["name"].as_str().unwrap_or("")).font(semibold(16.0)));
                ui.add(egui::Label::new(RichText::new(job_route(j)).font(sans(13.0)).color(MUTED)).truncate());
            });
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if button(ui, "Отменить", false, 100.0, 36.0).clicked() {
                    self.send(json!({"cmd": "cancel", "job": id}));
                }
                if button(ui, if paused { "Продолжить" } else { "Пауза" }, paused, 116.0, 36.0).clicked() {
                    self.send(json!({"cmd": if paused { "resume" } else { "pause" }, "job": id}));
                }
            });
        });
        ui.add_space(4.0);
        let samples: Vec<f64> = j["samples"].as_array().map(|a| a.iter().filter_map(|x| x.as_f64()).collect()).unwrap_or_default();
        ui.horizontal(|ui| {
            ui.set_height(64.0);
            let wide = ui.available_width();
            ui.allocate_ui_with_layout(Vec2::new(wide * 0.5, 64.0), Layout::top_down(Align::Min), |ui| {
                ui.set_width(wide * 0.5);
                if lost {
                    ui.label(RichText::new(format!("Связь с {} потеряна", j["peer_name"].as_str().unwrap_or("устройством"))).font(semibold(20.0)).color(BAD));
                    ui.label(RichText::new("Принятое сохранено, передача продолжится сама").font(sans(13.0)).color(MUTED));
                } else if matches!(phase, "seeding" | "verifying" | "repairing" | "confirming" | "finalizing") {
                    let (d, t) = (j["phase_done"].as_f64().unwrap_or(0.0), j["phase_total"].as_f64().unwrap_or(1.0));
                    let title = match phase {
                        "seeding" => format!("Ищу совпадающие блоки · {} %", comma(d / t.max(1.0) * 100.0, 0)),
                        "verifying" => format!("Сверяю с диска · {} %", comma(d / t.max(1.0) * 100.0, 0)),
                        "repairing" => "Передаю заново блоки, не прошедшие сверку".to_string(),
                        "confirming" => "Сравниваю итоговый хеш".to_string(),
                        _ => "Переношу файлы на место".to_string(),
                    };
                    ui.label(RichText::new(title).font(semibold(20.0)));
                } else if phase == "waiting_accept" {
                    ui.label(RichText::new("Жду согласия получателя").font(semibold(20.0)));
                } else {
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(comma(speed / 1e6, 1)).font(mono_med(44.0)).color(if paused { MUTED } else { accent() }));
                        ui.label(RichText::new("МБ/с").font(sans(16.0)).color(MUTED));
                    });
                    ui.label(RichText::new(format!("в среднем {}", comma(avg / 1e6, 1))).font(sans(13.0)).color(MUTED));
                }
            });
            graph(ui, &samples, avg, 64.0);
        });
        ui.add_space(8.0);
        bar(ui, (bytes / total.max(1.0)) as f32, 10.0, if lost || paused { MUTED } else { accent() });
        ui.horizontal(|ui| {
            ui.label(RichText::new(format!("{} из {} · {} %", size(bytes), size(total), comma(bytes / total.max(1.0) * 100.0, 1))).font(mono(13.0)));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                let t = if paused {
                    "на паузе".to_string()
                } else if let Some(e) = j["eta_secs"].as_f64() {
                    format!("ещё {}", duration(e))
                } else {
                    String::new()
                };
                ui.label(RichText::new(t).font(semibold(15.0)));
            });
        });
        ui.horizontal(|ui| {
            let retx = j["retx"].as_u64().unwrap_or(0);
            let sent = j["sent_pkts"].as_u64().unwrap_or(0).max(1);
            let rate = j["rate"].as_f64().unwrap_or(0.0);
            let is_send = j["role"] == "send";
            if is_send {
                ui.label(RichText::new(format!("повторно {} · {} %", spaced(retx), comma(retx as f64 / sent as f64 * 100.0, 2))).font(sans(12.5)).color(MUTED));
                if rate > 0.0 && !paused && !lost && phase == "transfer" {
                    ui.label(RichText::new(format!("предел {} МБ/с", comma(rate / 1e6, 0))).font(sans(12.5)).color(MUTED))
                        .on_hover_text("Приложение само подстраивает скорость под сеть: растит её, пока канал отвечает, и снижает, когда перестаёт.");
                }
            }
            ui.label(
                RichText::new(format!("блоки {} из {}", spaced(j["blocks_done"].as_u64().unwrap_or(0)), spaced(j["blocks_total"].as_u64().unwrap_or(0))))
                    .font(sans(12.5))
                    .color(MUTED),
            );
            let msg = j["message"].as_str().unwrap_or("");
            if !msg.is_empty() {
                ui.label(RichText::new(msg).font(sans(12.5)).color(MUTED));
            }
        });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            for (k, l) in [("all", "Все"), ("active", "В работе"), ("done", "Готово"), ("queue", "Ждут")] {
                if chip(ui, l, self.filter == k).clicked() {
                    self.filter = k.into();
                    if let Ok(mut g) = self.shared.lock() {
                        g.files = Value::Null;
                    }
                }
            }
            let tot = self.shared.lock().unwrap().files["total"].as_u64().unwrap_or(0);
            ui.label(RichText::new(plural(tot, "файл", "файла", "файлов")).font(sans(12.0)).color(MUTED));
        });
        ui.add_space(4.0);
        let files = self.shared.lock().unwrap().files["files"].as_array().cloned().unwrap_or_default();
        let h = (ui.available_height() - reserve).max(30.0);
        egui::ScrollArea::vertical().id_salt("files").max_height(h).auto_shrink([false, false]).show(ui, |ui| {
            for f in &files {
                let st = f["state"].as_str().unwrap_or("");
                let prog = f["progress"].as_f64().unwrap_or(0.0);
                let fg = if st == "queue" { MUTED } else { TEXT };
                ui.horizontal(|ui| {
                    ui.set_height(26.0);
                    let w = ui.available_width();
                    ui.allocate_ui_with_layout(Vec2::new(w - 310.0, 22.0), Layout::left_to_right(Align::Center), |ui| {
                        ui.set_width(w - 310.0);
                        ui.add(egui::Label::new(RichText::new(f["path"].as_str().unwrap_or("")).font(mono(12.5)).color(fg)).truncate());
                    });
                    ui.allocate_ui(Vec2::new(100.0, 22.0), |ui| bar(ui, prog as f32, 4.0, if st == "done" { OK } else { accent() }));
                    ui.add_sized([80.0, 22.0], egui::Label::new(RichText::new(size(f["size"].as_f64().unwrap_or(0.0))).font(mono(12.5)).color(fg)));
                    let (t, c) = match st {
                        "done" => ("хеш совпал".to_string(), OK),
                        "active" => (format!("{} %", comma(prog * 100.0, 1)), TEXT),
                        _ => ("ждёт".to_string(), MUTED),
                    };
                    ui.add_sized([90.0, 22.0], egui::Label::new(RichText::new(t).font(sans(12.5)).color(c)));
                });
            }
        });
    }

    /// Строка задания под панелью: очередь, ещё одна идущая передача или итог.
    fn job_row(&mut self, ui: &mut Ui, j: &Value) {
        let id = j["id"].as_str().unwrap_or("").to_string();
        let state = j["state"].as_str().unwrap_or("");
        let (bytes, total) = (j["bytes"].as_f64().unwrap_or(0.0), j["total"].as_f64().unwrap_or(0.0));
        let (status, col) = match state {
            "queued" => (format!("в очереди · {}", size(total)), MUTED),
            "done" => (format!("хеш совпал · {} · {} МБ/с", size(total), comma(j["avg_speed"].as_f64().unwrap_or(0.0) / 1e6, 1)), OK),
            "cancelled" => ("отменено".to_string(), MUTED),
            "declined" => (format!("получатель отказался {}", j["message"].as_str().unwrap_or("")), BAD),
            "failed" => (format!("не удалось: {}", j["message"].as_str().unwrap_or("")), BAD),
            _ => (format!("{} МБ/с · {} %", comma(j["speed"].as_f64().unwrap_or(0.0) / 1e6, 1), comma(bytes / total.max(1.0) * 100.0, 0)), accent()),
        };
        let mut close = false;
        ui.horizontal(|ui| {
            ui.set_height(ROW_H);
            let w = ui.available_width();
            ui.allocate_ui_with_layout(Vec2::new(w - 36.0, 28.0), Layout::left_to_right(Align::Center), |ui| {
                ui.set_width(w - 36.0);
                ui.label(RichText::new(j["name"].as_str().unwrap_or("")).font(semibold(13.0)));
                let r = ui.add(egui::Label::new(RichText::new(status).font(sans(13.0)).color(col)).truncate());
                if state == "done" {
                    let rep = j["repaired"].as_u64().unwrap_or(0);
                    r.on_hover_text(format!(
                        "{}\nЗа {}, передано заново после сверки: {}\nBLAKE3 {}",
                        job_route(j),
                        duration(j["active_secs"].as_f64().unwrap_or(0.0)),
                        rep,
                        j["root"].as_str().unwrap_or("")
                    ));
                }
            });
            if !matches!(state, "running" | "connecting" | "reconnecting" | "interrupted") {
                close = ui
                    .add(egui::Button::new("×").fill(PANEL).min_size(Vec2::splat(28.0)))
                    .on_hover_text(if state == "queued" { "Убрать из очереди" } else { "Скрыть" })
                    .clicked();
            }
        });
        if close {
            if state == "queued" {
                self.send(json!({"cmd": "cancel", "job": id}));
            }
            self.dismissed.push(id);
        }
    }
}

const ROW_H: f32 = 32.0;

fn is_finished(j: &Value) -> bool {
    matches!(j["state"].as_str().unwrap_or(""), "done" | "failed" | "cancelled" | "declined")
}

fn job_route(j: &Value) -> String {
    let peer = j["peer_name"].as_str().unwrap_or("");
    let dest = j["dest"].as_str().map(|d| format!(", {d}")).unwrap_or_default();
    format!("{} {peer}{dest}", if j["role"] == "send" { "→" } else { "←" })
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _f: &mut eframe::Frame) {
        let snapshot = self.shared.lock().unwrap().status.clone();
        egui::CentralPanel::default().frame(Frame::new().fill(GROUND).inner_margin(Margin::symmetric(20, 8))).show(ctx, |ui| {
            let Some(st) = snapshot else {
                ui.centered_and_justified(|ui| ui.label(RichText::new("Запускаю службу…").color(MUTED)));
                return;
            };
            // Запрос на сопряжение главнее всего: его нужно ввести, пока код жив.
            let pair_active = st["pair"]["status"].as_str().is_some_and(|s| s != "paired") || (self.adding);
            if pair_active {
                self.header_idle(ui, &st);
                self.pair_ui(ui, &st);
                return;
            }
            let offers = st["offers"].as_array().map(|a| a.len()).unwrap_or(0);
            // Служба отдаёт задания от новых к старым; панель показывает самое раннее идущее.
            let jobs: Vec<Value> = st["jobs"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|j| !self.dismissed.iter().any(|d| j["id"] == d.as_str()) && j["state"] != "offer")
                .collect();
            let main = jobs.iter().rev().find(|j| !is_finished(j) && j["state"] != "queued").cloned();
            let rows: Vec<Value> = jobs.iter().rev().filter(|j| j["id"] != main.as_ref().map_or(Value::Null, |m| m["id"].clone())).cloned().collect();
            let rows: Vec<Value> = rows.iter().filter(|j| !is_finished(j)).chain(rows.iter().rev().filter(|j| is_finished(j)).take(3)).cloned().collect();
            let reserve = rows.len() as f32 * (ROW_H + 4.0) + 8.0;
            self.header_idle(ui, &st);
            ui.add_space(6.0);
            if offers > 0 {
                self.receive_tab = true;
            }
            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(Vec2::new(280.0, ui.available_height()), Layout::top_down(Align::Min), |ui| {
                    ui.set_width(280.0);
                    if let Some(i) = segment(ui, &["Отправить", "Принять"], usize::from(self.receive_tab)) {
                        self.receive_tab = i == 1;
                    }
                    ui.add_space(12.0);
                    self.devices_panel(ui, &st);
                    if self.receive_tab {
                        ui.add_space(12.0);
                        if let Some(id) = self.device.clone() {
                            let d = st["devices"].as_array().and_then(|a| a.iter().find(|d| d["id"] == id.as_str()));
                            let mut on = d.is_some_and(|d| d["auto_accept"] == true);
                            if ui.checkbox(&mut on, "Принимать от этого устройства без вопроса").changed() {
                                self.send(json!({"cmd": "auto_accept", "peer": id, "on": on}));
                            }
                        }
                    }
                });
                ui.add_space(20.0);
                ui.allocate_ui_with_layout(ui.available_size(), Layout::top_down(Align::Min), |ui| {
                    // Запрос на приём занимает всю колонку: в нём есть решение, остальное видно строками.
                    if self.receive_tab && offers > 0 {
                        for j in main.iter().chain(rows.iter()) {
                            self.job_row(ui, j);
                        }
                        self.receive_tab_ui(ui, &st);
                        return;
                    }
                    if !self.receive_tab {
                        self.send_tab(ui, &st, main.is_some());
                    }
                    match &main {
                        Some(j) => self.transfer_panel(ui, j, reserve),
                        None if self.receive_tab => self.receive_tab_ui_idle(ui, reserve),
                        None => self.drop_zone(ui, reserve),
                    }
                    ui.add_space(8.0);
                    for j in &rows {
                        self.job_row(ui, j);
                    }
                });
            });
        });
        let err = std::mem::take(&mut self.shared.lock().unwrap().error);
        if !err.is_empty() {
            eprintln!("deckdrop: {err}");
        }
        ctx.request_repaint_after(Duration::from_millis(250));
    }
}

pub fn run() -> Result<(), String> {
    let (w, h) = if cfg!(target_os = "linux") { (1067.0, 667.0) } else { (1040.0, 720.0) };
    let opts = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_title("DeckDrop").with_inner_size([w, h]).with_min_inner_size([760.0, 520.0]).with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native("DeckDrop", opts, Box::new(|cc| Ok(Box::new(App::new(cc))))).map_err(|e| e.to_string())
}
