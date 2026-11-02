//! Локальное управление службой: строки JSON через Unix-сокет. Им пользуются окно, Decky, CLI и MCP.
use crate::node::{socket_path, Node};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn strs(v: &Value) -> Vec<String> {
    v.as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default()
}

fn handle(node: &Arc<Node>, r: &Value) -> Result<Value, String> {
    let s = |k: &str| r[k].as_str().unwrap_or("");
    match s("cmd") {
        "status" => Ok(node.status()),
        "ping" => Ok(json!({"ok": true})),
        "send" => node.send(
            s("peer"),
            strs(&r["paths"]).into_iter().map(PathBuf::from).collect(),
            r["dest"].as_str().map(String::from),
            r["overwrite"].as_bool().unwrap_or(false),
            r["from_llm"].as_bool().unwrap_or(false),
            r["in_place"].as_bool().unwrap_or(false),
        ),
        "pause" | "resume" | "cancel" => node.job_control(s("job"), s("cmd")),
        "files" => node.job_files(
            r["job"].as_str().unwrap_or("last"),
            r["offset"].as_u64().unwrap_or(0) as usize,
            r["limit"].as_u64().unwrap_or(200) as usize,
            r["filter"].as_str().unwrap_or("all"),
        ),
        "accept" => node.answer_offer(s("offer"), true, r["dest"].as_str(), strs(&r["exclude"])),
        "decline" => node.answer_offer(s("offer"), false, None, vec![]),
        "pair_start" => node.pair_start(s("peer")),
        "pair_enter" => node.pair_enter(s("code")),
        "unpair" => node.unpair(s("peer")),
        "auto_accept" => node.set_auto_accept(s("peer"), r["on"].as_bool().unwrap_or(false)),
        "scan" => node.scan_now(),
        other => Err(format!("неизвестная команда: {other}")),
    }
}

pub fn serve(node: Arc<Node>) -> std::io::Result<()> {
    let path = socket_path();
    if UnixStream::connect(&path).is_ok() {
        return Err(std::io::Error::other("служба уже запущена"));
    }
    let _ = std::fs::remove_file(&path);
    let l = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    for c in l.incoming() {
        let Ok(c) = c else { continue };
        let node = node.clone();
        let _ = std::thread::Builder::new().name("dd-ctl".into()).spawn(move || {
            let mut w = match c.try_clone() {
                Ok(w) => w,
                Err(_) => return,
            };
            for line in BufReader::new(c).lines() {
                let Ok(line) = line else { return };
                let resp = match serde_json::from_str::<Value>(&line) {
                    Ok(v) => match handle(&node, &v) {
                        Ok(x) => json!({"ok": true, "result": x}),
                        Err(e) => json!({"ok": false, "error": e}),
                    },
                    Err(e) => json!({"ok": false, "error": e.to_string()}),
                };
                if writeln!(w, "{resp}").is_err() {
                    return;
                }
            }
        });
    }
    Ok(())
}

/// Клиент службы.
pub struct Client {
    w: UnixStream,
    r: BufReader<UnixStream>,
}

impl Client {
    pub fn connect() -> std::io::Result<Self> {
        let s = UnixStream::connect(socket_path())?;
        s.set_read_timeout(Some(Duration::from_secs(10)))?;
        Ok(Self { r: BufReader::new(s.try_clone()?), w: s })
    }

    /// Подключается, при необходимости запуская службу.
    pub fn connect_or_spawn() -> std::io::Result<Self> {
        if let Ok(c) = Self::connect() {
            return Ok(c);
        }
        let exe = std::env::current_exe()?;
        use std::os::unix::process::CommandExt;
        let mut cmd = std::process::Command::new(exe);
        // Своя сессия: служба переживает закрытие окна и терминала.
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
        cmd.arg("daemon").stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn()?;
        for _ in 0..50 {
            std::thread::sleep(Duration::from_millis(100));
            if let Ok(c) = Self::connect() {
                return Ok(c);
            }
        }
        Err(std::io::Error::other("служба не запустилась"))
    }

    pub fn call(&mut self, req: Value) -> Result<Value, String> {
        writeln!(self.w, "{req}").map_err(|e| e.to_string())?;
        let mut line = String::new();
        self.r.read_line(&mut line).map_err(|e| e.to_string())?;
        let v: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["result"].clone())
        } else {
            Err(v["error"].as_str().unwrap_or("ошибка").to_string())
        }
    }
}
