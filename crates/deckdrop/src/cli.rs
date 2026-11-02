//! Командная строка и MCP-сервер.
use crate::ctl::Client;
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::time::Duration;

fn client() -> Result<Client, String> {
    Client::connect_or_spawn().map_err(|e| format!("служба недоступна: {e}"))
}

fn mbs(x: f64) -> String {
    format!("{:.1} МБ/с", x / 1e6)
}

fn print_job(j: &Value) {
    let (b, t) = (j["bytes"].as_f64().unwrap_or(0.0), j["total"].as_f64().unwrap_or(0.0));
    println!(
        "{} [{}] {} → {}: {} ({}/{}) {:.2} из {:.2} ГБ, сейчас {}, в среднем {}, повторов {}{}",
        j["id"].as_str().unwrap_or(""),
        j["role"].as_str().unwrap_or(""),
        j["name"].as_str().unwrap_or(""),
        j["peer_name"].as_str().unwrap_or(""),
        j["state"].as_str().unwrap_or(""),
        j["phase"].as_str().unwrap_or(""),
        if j["paused"].as_bool() == Some(true) { "пауза" } else { "идёт" },
        b / 1e9,
        t / 1e9,
        mbs(j["speed"].as_f64().unwrap_or(0.0)),
        mbs(j["avg_speed"].as_f64().unwrap_or(0.0)),
        j["retx"].as_u64().unwrap_or(0),
        if j["message"].as_str().unwrap_or("").is_empty() { String::new() } else { format!(" — {}", j["message"].as_str().unwrap()) },
    );
}

fn usage() -> String {
    "Использование:
  deckdrop                     открыть окно
  deckdrop daemon              запустить службу
  deckdrop status [--json]     устройства, задания, предложения
  deckdrop send <устройство> <путь>... [--to папка] [--overwrite] [--in-place] [--wait] [--json]
                               --in-place обновляет файлы в папке, передавая только изменившиеся блоки
  deckdrop pause|resume|cancel <задание|last>
  deckdrop files [задание] [--json]
  deckdrop accept <предложение> [--to папка]
  deckdrop decline <предложение>
  deckdrop pair <устройство>   показать код на этом устройстве
  deckdrop pair-enter <код>    ввести код на другом устройстве
  deckdrop auto-accept <устройство> on|off
  deckdrop install-service     запускать службу при входе (только для этого пользователя)
  deckdrop uninstall-service
  deckdrop mcp                 MCP-сервер на stdin/stdout"
        .into()
}

pub fn run(args: &[String]) -> Result<(), String> {
    let json_out = args.iter().any(|a| a == "--json");
    let pos: Vec<&String> = {
        let mut v = vec![];
        let mut i = 1;
        while i < args.len() {
            if args[i] == "--to" {
                i += 2;
                continue;
            }
            if !args[i].starts_with("--") {
                v.push(&args[i]);
            }
            i += 1;
        }
        v
    };
    let opt_to = args.iter().position(|a| a == "--to").and_then(|i| args.get(i + 1)).cloned();
    match args[0].as_str() {
        "status" => {
            let v = client()?.call(json!({"cmd": "status"}))?;
            if json_out {
                println!("{v}");
                return Ok(());
            }
            println!("Это устройство: {} ({})", v["me"]["name"].as_str().unwrap_or(""), v["me"]["id"].as_str().unwrap_or(""));
            for r in v["me"]["roots"].as_array().unwrap_or(&vec![]) {
                println!("  {}: свободно {:.1} ГБ", r["path"].as_str().unwrap_or(""), r["free"].as_f64().unwrap_or(0.0) / 1e9);
            }
            for d in v["devices"].as_array().unwrap_or(&vec![]) {
                println!(
                    "Устройство: {} {} {} {}",
                    d["name"].as_str().unwrap_or(""),
                    if d["paired"].as_bool() == Some(true) { "сопряжено" } else { "не сопряжено" },
                    if d["online"].as_bool() == Some(true) { "в сети" } else { "не в сети" },
                    d["addr"].as_str().unwrap_or("")
                );
                for r in d["roots"].as_array().unwrap_or(&vec![]) {
                    println!("  {}: свободно {:.1} ГБ", r["path"].as_str().unwrap_or(""), r["free"].as_f64().unwrap_or(0.0) / 1e9);
                }
            }
            for o in v["offers"].as_array().unwrap_or(&vec![]) {
                println!(
                    "Предложение {}: {} от {}, {:.2} ГБ, в {}",
                    o["offer"].as_str().unwrap_or(""),
                    o["name"].as_str().unwrap_or(""),
                    o["peer_name"].as_str().unwrap_or(""),
                    o["total"].as_f64().unwrap_or(0.0) / 1e9,
                    o["dest"].as_str().unwrap_or("")
                );
            }
            for j in v["jobs"].as_array().unwrap_or(&vec![]) {
                print_job(j);
            }
            if let Some(p) = v["pair"].as_object() {
                println!("Сопряжение: {:?}", p);
            }
            Ok(())
        }
        "send" => {
            if pos.len() < 2 {
                return Err(usage());
            }
            let paths: Vec<String> =
                pos[1..].iter().map(|p| std::fs::canonicalize(p).map(|x| x.to_string_lossy().into_owned()).unwrap_or_else(|_| p.to_string())).collect();
            let mut c = client()?;
            let r = c.call(json!({"cmd": "send", "peer": pos[0], "paths": paths, "dest": opt_to, "overwrite": args.iter().any(|a| a == "--overwrite"), "from_llm": args.iter().any(|a| a == "--from-llm"), "in_place": args.iter().any(|a| a == "--in-place")}))?;
            let id = r["job"].as_str().unwrap_or("").to_string();
            if !args.iter().any(|a| a == "--wait") {
                println!("{}", if json_out { r.to_string() } else { format!("задание {id}") });
                return Ok(());
            }
            loop {
                std::thread::sleep(Duration::from_secs(1));
                let v = c.call(json!({"cmd": "status"}))?;
                let Some(j) = v["jobs"].as_array().and_then(|a| a.iter().find(|j| j["id"] == id.as_str())) else {
                    return Err("задание пропало".into());
                };
                let mut brief = j.clone();
                brief.as_object_mut().map(|o| o.remove("samples"));
                if json_out {
                    println!("{brief}");
                } else {
                    print_job(j);
                }
                match j["state"].as_str().unwrap_or("") {
                    "done" => return Ok(()),
                    "failed" | "cancelled" | "declined" => return Err(j["message"].as_str().unwrap_or("не удалось").to_string()),
                    _ => {}
                }
            }
        }
        "pause" | "resume" | "cancel" => {
            let job = pos.first().map(|s| s.as_str()).unwrap_or("last");
            println!("{}", client()?.call(json!({"cmd": args[0], "job": job}))?);
            Ok(())
        }
        "files" => {
            let job = pos.first().map(|s| s.as_str()).unwrap_or("last");
            let v = client()?.call(json!({"cmd": "files", "job": job, "limit": 100000}))?;
            if json_out {
                println!("{v}");
            } else {
                for f in v["files"].as_array().unwrap_or(&vec![]) {
                    println!(
                        "{:>5.1} % {:>12} {}",
                        f["progress"].as_f64().unwrap_or(0.0) * 100.0,
                        f["size"].as_u64().unwrap_or(0),
                        f["path"].as_str().unwrap_or("")
                    );
                }
            }
            Ok(())
        }
        "accept" | "decline" => {
            let offer = pos.first().ok_or_else(usage)?;
            println!("{}", client()?.call(json!({"cmd": args[0], "offer": offer, "dest": opt_to}))?);
            Ok(())
        }
        "pair" => {
            let peer = pos.first().ok_or_else(usage)?;
            let mut c = client()?;
            let r = c.call(json!({"cmd": "pair_start", "peer": peer}))?;
            println!("Введите на другом устройстве код: {}", r["code"].as_str().unwrap_or(""));
            for _ in 0..240 {
                std::thread::sleep(Duration::from_millis(500));
                let v = c.call(json!({"cmd": "status"}))?;
                match v["pair"]["status"].as_str() {
                    Some("paired") => {
                        println!("Готово, устройства сопряжены");
                        return Ok(());
                    }
                    Some("bad_code") => return Err("код не подошёл".into()),
                    None => return Err("время истекло".into()),
                    _ => {}
                }
            }
            Err("время истекло".into())
        }
        "pair-enter" => {
            let code = pos.first().ok_or_else(usage)?;
            let mut c = client()?;
            for _ in 0..40 {
                let v = c.call(json!({"cmd": "status"}))?;
                if v["pair"]["status"].as_str() == Some("enter_code") {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            c.call(json!({"cmd": "pair_enter", "code": code}))?;
            for _ in 0..40 {
                std::thread::sleep(Duration::from_millis(250));
                let v = c.call(json!({"cmd": "status"}))?;
                match v["pair"]["status"].as_str() {
                    Some("paired") => {
                        println!("Готово, устройства сопряжены");
                        return Ok(());
                    }
                    Some("bad_code") => return Err("код не подошёл".into()),
                    _ => {}
                }
            }
            Err("другое устройство не ответило".into())
        }
        "auto-accept" => {
            if pos.len() < 2 {
                return Err(usage());
            }
            println!("{}", client()?.call(json!({"cmd": "auto_accept", "peer": pos[0], "on": pos[1] == "on"}))?);
            Ok(())
        }
        "mcp" => mcp(),
        "install-service" => install_service(),
        "uninstall-service" => uninstall_service(),
        _ => Err(usage()),
    }
}

fn tool(name: &str, desc: &str, props: Value, req: &[&str]) -> Value {
    json!({"name": name, "description": desc, "inputSchema": {"type": "object", "properties": props, "required": req}})
}

fn tools() -> Value {
    json!([
        tool("status", "Устройства в сети, свободное место на них, текущие задания и предложения файлов.", json!({}), &[]),
        tool("send", "Отправить файлы или папки на сопряжённое устройство. Без overwrite существующие файлы не заменяются; получатель может отказать. Если к устройству уже идёт отправка, задание встаёт в очередь (state=queued) и начнётся после неё; убрать из очереди можно через cancel.", json!({
            "peer": {"type": "string", "description": "Имя или id устройства"},
            "paths": {"type": "array", "items": {"type": "string"}, "description": "Абсолютные пути на этом устройстве"},
            "dest": {"type": "string", "description": "Папка на получателе, например ~/Games"},
            "overwrite": {"type": "boolean", "description": "Разрешить замену существующих файлов"},
            "in_place": {"type": "boolean", "description": "Обновить файлы прямо в папке назначения, передав только изменившиеся блоки. Получатель подтверждает вручную."}
        }), &["peer", "paths"]),
        tool("pause", "Поставить задание на паузу.", json!({"job": {"type": "string"}}), &["job"]),
        tool("resume", "Продолжить задание.", json!({"job": {"type": "string"}}), &["job"]),
        tool("cancel", "Отменить задание. Частично принятые данные будут удалены.", json!({"job": {"type": "string"}}), &["job"]),
        tool("files", "Прогресс по файлам задания.", json!({
            "job": {"type": "string"}, "offset": {"type": "integer"}, "limit": {"type": "integer"},
            "filter": {"type": "string", "enum": ["all", "active", "done", "queue"]}
        }), &["job"]),
        tool("accept", "Принять предложение файлов.", json!({
            "offer": {"type": "string"}, "dest": {"type": "string"},
            "exclude": {"type": "array", "items": {"type": "string"}}
        }), &["offer"]),
        tool("decline", "Отклонить предложение файлов.", json!({"offer": {"type": "string"}}), &["offer"]),
    ])
}

/// Сопряжения здесь намеренно нет: новое устройство добавляет человек, вводя код.
fn mcp() -> Result<(), String> {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line.map_err(|e| e.to_string())?;
        let Ok(req) = serde_json::from_str::<Value>(&line) else { continue };
        let id = req.get("id").cloned();
        let method = req["method"].as_str().unwrap_or("");
        let result = match method {
            "initialize" => Some(json!({
                "protocolVersion": req["params"]["protocolVersion"].as_str().unwrap_or("2025-06-18"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "deckdrop", "version": env!("CARGO_PKG_VERSION")},
            })),
            "ping" => Some(json!({})),
            "tools/list" => Some(json!({"tools": tools()})),
            "tools/call" => {
                let name = req["params"]["name"].as_str().unwrap_or("");
                let mut a = req["params"]["arguments"].clone();
                if !a.is_object() {
                    a = json!({});
                }
                a["cmd"] = json!(name);
                if name == "send" {
                    a["from_llm"] = json!(true);
                }
                let known = tools().as_array().unwrap().iter().any(|t| t["name"] == name);
                let r = if known { client().and_then(|mut c| c.call(a)) } else { Err(format!("неизвестный инструмент: {name}")) };
                Some(match r {
                    Ok(v) => json!({"content": [{"type": "text", "text": v.to_string()}]}),
                    Err(e) => json!({"content": [{"type": "text", "text": e}], "isError": true}),
                })
            }
            _ => None,
        };
        let Some(id) = id else { continue };
        let resp = match result {
            Some(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
            None => json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "нет такого метода"}}),
        };
        writeln!(out, "{resp}").map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn run_cmd(prog: &str, args: &[&str]) -> Result<(), String> {
    let st = std::process::Command::new(prog).args(args).status().map_err(|e| format!("{prog}: {e}"))?;
    if st.success() {
        Ok(())
    } else {
        Err(format!("{prog} {} завершился с ошибкой", args.join(" ")))
    }
}

fn service_file() -> std::path::PathBuf {
    let home = crate::node::home_dir();
    if cfg!(target_os = "macos") {
        home.join("Library/LaunchAgents/com.deckdrop.daemon.plist")
    } else {
        home.join(".config/systemd/user/deckdrop.service")
    }
}

fn install_service() -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let exe = exe.display();
    let path = service_file();
    std::fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
    if cfg!(target_os = "macos") {
        let plist = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>com.deckdrop.daemon</string>
<key>ProgramArguments</key><array><string>{exe}</string><string>daemon</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
</dict></plist>
"#
        );
        std::fs::write(&path, plist).map_err(|e| e.to_string())?;
        let p = path.to_string_lossy().into_owned();
        let _ = std::process::Command::new("launchctl").args(["unload", &p]).status();
        run_cmd("launchctl", &["load", &p])?;
    } else {
        let unit =
            format!("[Unit]\nDescription=DeckDrop\n\n[Service]\nExecStart={exe} daemon\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n");
        std::fs::write(&path, unit).map_err(|e| e.to_string())?;
        run_cmd("systemctl", &["--user", "daemon-reload"])?;
        run_cmd("systemctl", &["--user", "enable", "--now", "deckdrop.service"])?;
        run_cmd("systemctl", &["--user", "restart", "deckdrop.service"])?;
    }
    println!("служба установлена: {}", path.display());
    Ok(())
}

fn uninstall_service() -> Result<(), String> {
    let path = service_file();
    if cfg!(target_os = "macos") {
        let p = path.to_string_lossy().into_owned();
        let _ = std::process::Command::new("launchctl").args(["unload", &p]).status();
    } else {
        let _ = std::process::Command::new("systemctl").args(["--user", "disable", "--now", "deckdrop.service"]).status();
    }
    let _ = std::fs::remove_file(&path);
    println!("служба удалена");
    Ok(())
}
