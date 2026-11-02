//! Служба: устройства в сети, сопряжение, сессии и задания.
use crate::channel::{Channel, T_BEAT};
use crate::crypto::{self, hex, unhex};
use crate::engine::{self, Decision, OfferInfo, Outcome, SendPlan, Shared};
use crate::stream::{self, Manifest, BLOCK};
use ddnet::UdpSock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAGIC: &[u8] = b"DDS1";
const ONLINE_FOR: Duration = Duration::from_secs(8);
const PAIR_CODE_TTL: Duration = Duration::from_secs(120);

pub fn data_dir() -> PathBuf {
    if let Ok(d) = std::env::var("DECKDROP_HOME") {
        return PathBuf::from(d);
    }
    let home = home_dir();
    if cfg!(target_os = "macos") {
        home.join("Library/Application Support/DeckDrop")
    } else {
        home.join(".local/share/deckdrop")
    }
}

pub fn home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/".into()))
}

pub fn socket_path() -> PathBuf {
    let p = data_dir().join("ctl.sock");
    if p.as_os_str().len() < 100 {
        return p;
    }
    // Адрес Unix-сокета ограничен примерно сотней байт, а путь к данным может быть длиннее.
    let tag = hex(&blake3::hash(p.as_os_str().as_encoded_bytes()).as_bytes()[..8]);
    std::env::temp_dir().join(format!("deckdrop-{tag}.sock"))
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn computer_name() -> String {
    if let Ok(n) = std::env::var("DECKDROP_NAME") {
        return n;
    }
    if cfg!(target_os = "macos") {
        if let Ok(o) = std::process::Command::new("scutil").args(["--get", "ComputerName"]).output() {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }
    let mut b = [0u8; 256];
    unsafe { libc::gethostname(b.as_mut_ptr().cast(), b.len()) };
    let host = String::from_utf8_lossy(&b).trim_end_matches('\0').to_string();
    if host == "steamdeck" {
        "Steam Deck".into()
    } else {
        host
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Identity {
    pub id: String,
    pub name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PeerRec {
    pub id: String,
    pub name: String,
    pub psk: String,
    #[serde(default)]
    pub addr: Option<SocketAddr>,
    #[serde(default)]
    pub auto_accept: bool,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct JobFile {
    job: String,
    peer: String,
    paths: Vec<PathBuf>,
    dest: Option<String>,
    overwrite: bool,
    from_llm: bool,
    name: String,
    finished: bool,
    #[serde(default)]
    in_place: bool,
    #[serde(default)]
    seq: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct AcceptedFile {
    job: String,
    dest: PathBuf,
    exclude: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "m", rename_all = "snake_case")]
enum Msg {
    Hello { id: String, name: String, port: u16, reply: bool },
    Sealed { from: String, nonce: u64, blob: String },
    PairInvite { from: String, name: String, port: u16 },
    PairMsg { from: String, name: String, port: u16, spake: String },
    PairConfirm { from: String, mac: String },
    SessReq { from: String, session: u32, port: u16, eph: String, nonce: String, mac: String },
    SessAcc { session: u32, port: u16, eph: String, mac: String },
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
struct StatusBody {
    roots: Vec<Root>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Root {
    pub path: String,
    pub free: u64,
}

struct Seen {
    name: String,
    addr: SocketAddr,
    at: Instant,
    roots: Vec<Root>,
}

/// Текущее сопряжение: `code` на показывающей стороне или ожидание ввода на другой.
struct PairState {
    peer: String,
    peer_name: String,
    peer_addr: SocketAddr,
    code: Option<String>,
    started: Instant,
    spake: Option<crypto::PairInit>,
    key: Option<[u8; 32]>,
    role_a: bool,
    status: String,
}

struct Job {
    id: String,
    role: &'static str,
    peer: String,
    peer_name: String,
    name: String,
    shared: Arc<Shared>,
    state: String,
    message: String,
    from_llm: bool,
    dest: Option<String>,
    started: Instant,
    active_secs: f64,
    bytes_at_start: Option<u64>,
    last_bytes: u64,
    last_tick: Instant,
    speed: f64,
    samples: VecDeque<f64>,
    finished_at: Option<u64>,
}

#[derive(Clone, Serialize)]
struct PendingOffer {
    offer: String,
    job: String,
    peer: String,
    peer_name: String,
    name: String,
    items: Vec<stream::Item>,
    total: u64,
    dest: String,
    overwrite: bool,
    from_llm: bool,
    in_place: bool,
    conflict: Vec<String>,
}

struct NodeInner {
    me: Identity,
    port: u16,
    peers: HashMap<String, PeerRec>,
    seen: HashMap<String, Seen>,
    jobs: HashMap<String, Job>,
    order: Vec<String>,
    /// Отправки идут к одному устройству по очереди: (задание, устройство).
    send_queue: Vec<(String, String)>,
    pending: HashMap<String, PendingOffer>,
    decisions: HashMap<String, Decision>,
    accepted: HashMap<String, AcceptedFile>,
    pair: Option<PairState>,
    scanning_until: Instant,
    sessions: HashMap<u32, (u16, String, String)>,
    inhibitor: Option<std::process::Child>,
    pending_session: HashMap<u32, std::sync::mpsc::Sender<Msg>>,
}

pub struct Node {
    inner: Mutex<NodeInner>,
    svc: UdpSock,
}

fn write_private(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(d) = path.parent() {
        fs::create_dir_all(d)?;
    }
    let tmp = path.with_extension("tmp");
    let mut f = fs::OpenOptions::new().create(true).write(true).truncate(true).mode(0o600).open(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(tmp, path)
}

fn local_ipv4() -> Vec<(Ipv4Addr, Ipv4Addr)> {
    let mut out = vec![];
    unsafe {
        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap) != 0 {
            return out;
        }
        let mut p = ifap;
        while !p.is_null() {
            let i = &*p;
            p = i.ifa_next;
            if i.ifa_addr.is_null() || i.ifa_netmask.is_null() || (i.ifa_flags & libc::IFF_LOOPBACK as u32) != 0 || (i.ifa_flags & libc::IFF_UP as u32) == 0 {
                continue;
            }
            if (*i.ifa_addr).sa_family as i32 != libc::AF_INET {
                continue;
            }
            let a = &*(i.ifa_addr as *const libc::sockaddr_in);
            let m = &*(i.ifa_netmask as *const libc::sockaddr_in);
            out.push((Ipv4Addr::from(u32::from_be(a.sin_addr.s_addr)), Ipv4Addr::from(u32::from_be(m.sin_addr.s_addr))));
        }
        libc::freeifaddrs(ifap);
    }
    out
}

/// Адреса для подбора значения по умолчанию: папки назначения этой системы.
fn default_roots() -> Vec<PathBuf> {
    let h = home_dir();
    let mut v: Vec<PathBuf> = ["Games", "Downloads", "Documents", "Desktop", "Pictures"].iter().map(|x| h.join(x)).filter(|p| p.is_dir()).collect();
    if cfg!(target_os = "linux") {
        if let Ok(rd) = fs::read_dir("/run/media") {
            for e in rd.flatten() {
                let p = e.path();
                if let Ok(kids) = fs::read_dir(&p) {
                    let mut has_user_dirs = false;
                    for k in kids.flatten() {
                        if k.path().is_dir() {
                            v.push(k.path());
                            has_user_dirs = true;
                        }
                    }
                    if !has_user_dirs {
                        v.push(p);
                    }
                }
            }
        }
    }
    if v.is_empty() {
        v.push(h);
    }
    v
}

fn expand_dest(s: &str) -> PathBuf {
    if let Some(r) = s.strip_prefix("~/") {
        home_dir().join(r)
    } else if s == "~" {
        home_dir()
    } else {
        PathBuf::from(s)
    }
}

fn short_home(p: &Path) -> String {
    match p.strip_prefix(home_dir()) {
        Ok(r) if r.as_os_str().is_empty() => "~".into(),
        Ok(r) => format!("~/{}", r.display()),
        Err(_) => p.display().to_string(),
    }
}

/// Папка разрешена для приёма без вопроса: внутри стандартных папок и без скрытых компонентов.
fn dest_allowed(dest: &Path) -> bool {
    let Some(root) = default_roots().into_iter().find(|r| dest.starts_with(r)) else {
        return false;
    };
    let rest = dest.strip_prefix(&root).unwrap_or(dest);
    !rest.components().any(|c| {
        let s = c.as_os_str().to_string_lossy();
        s.starts_with('.') || s == ".."
    })
}

fn sealed_key(psk_hex: &str) -> Option<crypto::DirKey> {
    Some(crypto::status_key(&unhex(psk_hex)?))
}

impl Node {
    pub fn start() -> std::io::Result<Arc<Node>> {
        let dir = data_dir();
        fs::create_dir_all(&dir)?;
        let idp = dir.join("identity.json");
        let me: Identity = match fs::read(&idp).ok().and_then(|d| serde_json::from_slice(&d).ok()) {
            Some(i) => i,
            None => {
                let i = Identity { id: hex(&crypto::random_bytes::<8>()), name: computer_name() };
                write_private(&idp, &serde_json::to_vec_pretty(&i)?)?;
                i
            }
        };
        let port: u16 = std::env::var("DECKDROP_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(52790);
        let svc = UdpSock::bind(format!("0.0.0.0:{port}").parse().unwrap(), 1 << 20, 1 << 20)?;
        unsafe {
            let on: libc::c_int = 1;
            libc::setsockopt(svc.fd(), libc::SOL_SOCKET, libc::SO_BROADCAST, (&on as *const libc::c_int).cast(), 4);
        }
        svc.set_recv_timeout(Some(Duration::from_millis(500)))?;
        let peers: Vec<PeerRec> = fs::read(dir.join("peers.json")).ok().and_then(|d| serde_json::from_slice(&d).ok()).unwrap_or_default();
        let accepted: Vec<AcceptedFile> = fs::read(dir.join("accepted.json")).ok().and_then(|d| serde_json::from_slice(&d).ok()).unwrap_or_default();
        let node = Arc::new(Node {
            inner: Mutex::new(NodeInner {
                me,
                port,
                peers: peers.into_iter().map(|p| (p.id.clone(), p)).collect(),
                seen: HashMap::new(),
                jobs: HashMap::new(),
                order: vec![],
                send_queue: vec![],
                pending: HashMap::new(),
                decisions: HashMap::new(),
                accepted: accepted.into_iter().map(|a| (a.job.clone(), a)).collect(),
                pair: None,
                scanning_until: Instant::now() + Duration::from_secs(30),
                sessions: HashMap::new(),
                inhibitor: None,
                pending_session: HashMap::new(),
            }),
            svc,
        });
        {
            let n = node.clone();
            std::thread::Builder::new().name("dd-svc".into()).spawn(move || n.service_loop())?;
            let n = node.clone();
            std::thread::Builder::new().name("dd-beacon".into()).spawn(move || n.beacon_loop())?;
            let n = node.clone();
            std::thread::Builder::new().name("dd-tick".into()).spawn(move || n.tick_loop())?;
        }
        node.resume_saved_jobs();
        Ok(node)
    }

    fn save_peers(&self, g: &NodeInner) {
        let v: Vec<&PeerRec> = g.peers.values().collect();
        if let Ok(d) = serde_json::to_vec_pretty(&v) {
            let _ = write_private(&data_dir().join("peers.json"), &d);
        }
    }

    fn save_accepted(&self, g: &NodeInner) {
        let v: Vec<&AcceptedFile> = g.accepted.values().collect();
        if let Ok(d) = serde_json::to_vec_pretty(&v) {
            let _ = write_private(&data_dir().join("accepted.json"), &d);
        }
    }

    fn send_msg(&self, m: &Msg, to: SocketAddr) {
        let mut b = MAGIC.to_vec();
        b.extend_from_slice(&serde_json::to_vec(m).unwrap_or_default());
        let _ = self.svc.send_to(&b, to);
    }

    // ---------- обнаружение ----------

    fn beacon_loop(self: Arc<Self>) {
        let mut round = 0u64;
        let test_peers: Vec<SocketAddr> =
            std::env::var("DECKDROP_PEERS").ok().map(|v| v.split(',').filter_map(|x| x.parse().ok()).collect()).unwrap_or_default();
        loop {
            let (hello, port, scanning, known, sealed) = {
                let g = self.inner.lock().unwrap();
                let hello = Msg::Hello { id: g.me.id.clone(), name: g.me.name.clone(), port: g.port, reply: false };
                let any_offline = g.peers.keys().any(|id| !g.seen.get(id).is_some_and(|s| s.at.elapsed() < ONLINE_FOR));
                let scanning = Instant::now() < g.scanning_until || any_offline;
                let known: Vec<SocketAddr> = g.peers.values().filter_map(|p| g.seen.get(&p.id).map(|s| s.addr).or(p.addr)).collect();
                let body = StatusBody { roots: this_roots() };
                let mut sealed = vec![];
                for p in g.peers.values() {
                    let Some(addr) = g.seen.get(&p.id).map(|s| s.addr).or(p.addr) else { continue };
                    let Some(k) = sealed_key(&p.psk) else { continue };
                    let mut blob = serde_json::to_vec(&body).unwrap_or_default();
                    let nonce = u64::from_le_bytes(crypto::random_bytes::<8>());
                    let tag = k.seal(nonce, g.me.id.as_bytes(), &mut blob);
                    blob.extend_from_slice(&tag);
                    sealed.push((Msg::Sealed { from: g.me.id.clone(), nonce, blob: hex(&blob) }, addr));
                }
                (hello, g.port, scanning, known, sealed)
            };
            let ifs = local_ipv4();
            for (m, a) in sealed {
                self.send_msg(&m, a);
            }
            for a in known.iter().chain(test_peers.iter()) {
                self.send_msg(&hello, *a);
            }
            if test_peers.is_empty() {
                self.send_msg(&hello, SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), port));
                for (ip, mask) in &ifs {
                    let (i, m) = (u32::from(*ip), u32::from(*mask));
                    let bc = Ipv4Addr::from(i | !m);
                    self.send_msg(&hello, SocketAddr::new(IpAddr::V4(bc), port));
                    // Если широковещание режут, раз в 20 с опрашиваем подсеть адресно.
                    if scanning && round.is_multiple_of(10) && m.count_zeros() <= 10 {
                        for h in 1..(1u32 << m.count_zeros()) - 1 {
                            let a = Ipv4Addr::from((i & m) | h);
                            if a != *ip {
                                self.send_msg(&hello, SocketAddr::new(IpAddr::V4(a), port));
                            }
                        }
                    }
                }
            }
            round += 1;
            std::thread::sleep(Duration::from_secs(2));
        }
    }

    fn service_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 16384];
        loop {
            let (n, from) = match self.svc.recv_from(&mut buf) {
                Ok(x) => x,
                Err(_) => continue,
            };
            if n < MAGIC.len() || &buf[..MAGIC.len()] != MAGIC {
                continue;
            }
            let Ok(m) = serde_json::from_slice::<Msg>(&buf[MAGIC.len()..n]) else { continue };
            self.clone().on_msg(m, from);
        }
    }

    fn on_msg(self: Arc<Self>, m: Msg, from: SocketAddr) {
        match m {
            Msg::Hello { id, name, port, reply } => {
                let mut g = self.inner.lock().unwrap();
                if id == g.me.id {
                    return;
                }
                let addr = SocketAddr::new(from.ip(), port);
                // Адрес сопряжённого устройства обновляет только запечатанное сообщение.
                let e = g.seen.entry(id.clone()).or_insert(Seen { name: name.clone(), addr, at: Instant::now(), roots: vec![] });
                e.name = name;
                e.at = Instant::now();
                if !g.peers.contains_key(&id) {
                    g.seen.get_mut(&id).unwrap().addr = addr;
                }
                if !reply {
                    let r = Msg::Hello { id: g.me.id.clone(), name: g.me.name.clone(), port: g.port, reply: true };
                    drop(g);
                    self.send_msg(&r, addr);
                }
            }
            Msg::Sealed { from: peer, nonce, blob } => {
                let mut g = self.inner.lock().unwrap();
                let Some(p) = g.peers.get(&peer).cloned() else { return };
                let (Some(k), Some(mut b)) = (sealed_key(&p.psk), unhex(&blob)) else { return };
                let Some(n) = k.open(nonce, peer.as_bytes(), &mut b) else { return };
                let Ok(body) = serde_json::from_slice::<StatusBody>(&b[..n]) else { return };
                let addr = SocketAddr::new(from.ip(), from.port());
                let e = g.seen.entry(peer.clone()).or_insert(Seen { name: p.name.clone(), addr, at: Instant::now(), roots: vec![] });
                e.addr = addr;
                e.at = Instant::now();
                e.roots = body.roots;
                if p.addr != Some(addr) {
                    g.peers.get_mut(&peer).unwrap().addr = Some(addr);
                    self.save_peers(&g);
                }
            }
            Msg::PairInvite { from: id, name, port } => {
                let mut g = self.inner.lock().unwrap();
                let addr = SocketAddr::new(from.ip(), port);
                g.pair = Some(PairState {
                    peer: id,
                    peer_name: name,
                    peer_addr: addr,
                    code: None,
                    started: Instant::now(),
                    spake: None,
                    key: None,
                    role_a: false,
                    status: "enter_code".into(),
                });
            }
            Msg::PairMsg { from: id, name, port, spake } => self.on_pair_msg(id, name, SocketAddr::new(from.ip(), port), spake),
            Msg::PairConfirm { from: id, mac } => self.on_pair_confirm(id, mac),
            Msg::SessReq { from: peer, session, port, eph, nonce, mac } => self.on_sess_req(peer, session, port, eph, nonce, mac, from),
            Msg::SessAcc { session, port, eph, mac } => {
                let g = self.inner.lock().unwrap();
                if let Some(tx) = g.pending_session.get(&session) {
                    let _ = tx.send(Msg::SessAcc { session, port, eph, mac });
                }
            }
        }
    }

    // ---------- сопряжение ----------

    /// Показывающая код сторона: приглашает устройство и ждёт его сообщения.
    pub fn pair_start(&self, peer: &str) -> Result<Value, String> {
        let mut g = self.inner.lock().unwrap();
        let (id, s) = g
            .seen
            .iter()
            .find(|(id, s)| id.as_str() == peer || s.name == peer)
            .map(|(i, s)| (i.clone(), (s.name.clone(), s.addr)))
            .ok_or("устройство не найдено в сети")?;
        let n = u32::from_le_bytes(crypto::random_bytes::<4>()) % 1_000_000;
        let code = format!("{n:06}");
        let (init, _msg) = crypto::pair_begin(&code, true);
        g.pair = Some(PairState {
            peer: id.clone(),
            peer_name: s.0.clone(),
            peer_addr: s.1,
            code: Some(code.clone()),
            started: Instant::now(),
            spake: Some(init),
            key: None,
            role_a: true,
            status: "show_code".into(),
        });
        let inv = Msg::PairInvite { from: g.me.id.clone(), name: g.me.name.clone(), port: g.port };
        drop(g);
        self.send_msg(&inv, s.1);
        Ok(json!({"code": code, "peer": s.0, "ttl_secs": PAIR_CODE_TTL.as_secs()}))
    }

    /// Вводящая код сторона: начинает обмен SPAKE2.
    pub fn pair_enter(&self, code: &str) -> Result<Value, String> {
        let mut g = self.inner.lock().unwrap();
        let (me, name, port) = (g.me.id.clone(), g.me.name.clone(), g.port);
        let p = g.pair.as_mut().ok_or("нет запроса на сопряжение")?;
        if p.started.elapsed() > PAIR_CODE_TTL {
            g.pair = None;
            return Err("время кода истекло".into());
        }
        let (init, msg) = crypto::pair_begin(code, false);
        p.spake = Some(init);
        p.status = "checking".into();
        let addr = p.peer_addr;
        drop(g);
        self.send_msg(&Msg::PairMsg { from: me, name, port, spake: hex(&msg) }, addr);
        Ok(json!({"status": "checking"}))
    }

    fn on_pair_msg(&self, id: String, name: String, addr: SocketAddr, spake: String) {
        let mut g = self.inner.lock().unwrap();
        let me = g.me.clone();
        let port = g.port;
        let Some(p) = g.pair.as_mut() else { return };
        if p.peer != id || p.started.elapsed() > PAIR_CODE_TTL {
            return;
        }
        let Some(peer_msg) = unhex(&spake) else { return };
        if p.role_a {
            // показывающая сторона: отвечает своим сообщением и вычисляет ключ
            let Some(code) = p.code.clone() else { return };
            let (init, _) = crypto::pair_begin(&code, true);
            // Второе сообщение сторона А создаёт заново: прежнее не отправлялось.
            let (init2, my) = crypto::pair_begin(&code, true);
            drop(init);
            p.key = crypto::pair_finish(init2, &peer_msg);
            p.peer_name = name;
            p.peer_addr = addr;
            if p.key.is_none() {
                g.pair = None;
                return;
            }
            drop(g);
            self.send_msg(&Msg::PairMsg { from: me.id, name: me.name, port, spake: hex(&my) }, addr);
        } else {
            let Some(init) = p.spake.take() else { return };
            let Some(key) = crypto::pair_finish(init, &peer_msg) else {
                p.status = "bad_code".into();
                return;
            };
            p.key = Some(key);
            let m = crypto::mac(&key, &[b"confirm-b"]);
            drop(g);
            self.send_msg(&Msg::PairConfirm { from: me.id, mac: hex(&m) }, addr);
        }
    }

    fn on_pair_confirm(&self, id: String, mac_hex: String) {
        let mut g = self.inner.lock().unwrap();
        let me = g.me.clone();
        let Some(p) = g.pair.as_mut() else { return };
        if p.peer != id {
            return;
        }
        let Some(key) = p.key else { return };
        let Some(m) = unhex(&mac_hex) else { return };
        let label: &[u8] = if p.role_a { b"confirm-b" } else { b"confirm-a" };
        if !crypto::mac_ok(&key, &[label], &m) {
            // Неверный код. Показанный код больше не годится: одна попытка на код.
            if p.role_a {
                p.status = "bad_code".into();
                p.code = None;
            } else {
                p.status = "bad_code".into();
            }
            p.key = None;
            return;
        }
        let (addr, role_a, name) = (p.peer_addr, p.role_a, p.peer_name.clone());
        p.status = "paired".into();
        let psk = crypto::hkdf32(&key, b"", b"deckdrop psk");
        g.peers.insert(id.clone(), PeerRec { id: id.clone(), name, psk: hex(&psk), addr: Some(addr), auto_accept: false });
        self.save_peers(&g);
        drop(g);
        if role_a {
            let mm = crypto::mac(&key, &[b"confirm-a"]);
            self.send_msg(&Msg::PairConfirm { from: me.id, mac: hex(&mm) }, addr);
        }
    }

    pub fn unpair(&self, peer: &str) -> Result<Value, String> {
        let mut g = self.inner.lock().unwrap();
        let id = g.peers.values().find(|p| p.id == peer || p.name == peer).map(|p| p.id.clone()).ok_or("нет такого устройства")?;
        g.peers.remove(&id);
        self.save_peers(&g);
        Ok(json!({"ok": true}))
    }

    pub fn set_auto_accept(&self, peer: &str, on: bool) -> Result<Value, String> {
        let mut g = self.inner.lock().unwrap();
        let id = g.peers.values().find(|p| p.id == peer || p.name == peer).map(|p| p.id.clone()).ok_or("нет такого устройства")?;
        g.peers.get_mut(&id).unwrap().auto_accept = on;
        self.save_peers(&g);
        Ok(json!({"ok": true, "auto_accept": on}))
    }

    pub fn scan_now(&self) -> Result<Value, String> {
        self.inner.lock().unwrap().scanning_until = Instant::now() + Duration::from_secs(60);
        Ok(json!({"ok": true}))
    }

    // ---------- сессии ----------

    fn on_sess_req(self: Arc<Self>, peer: String, session: u32, peer_port: u16, eph: String, nonce: String, mac: String, from: SocketAddr) {
        let (psk, name) = {
            let g = self.inner.lock().unwrap();
            let Some(p) = g.peers.get(&peer) else { return };
            (p.psk.clone(), p.name.clone())
        };
        let Some(pskb) = unhex(&psk) else { return };
        let (Some(eph_b), Some(nb), Some(mb)) = (unhex(&eph), unhex(&nonce), unhex(&mac)) else { return };
        if !crypto::mac_ok(&pskb, &[b"req", &session.to_le_bytes(), &peer_port.to_le_bytes(), &eph_b, &nb, peer.as_bytes()], &mb) {
            return;
        }
        let dup = self.inner.lock().unwrap().sessions.get(&session).cloned();
        if let Some((port, e, m)) = dup {
            self.send_msg(&Msg::SessAcc { session, port, eph: e, mac: m }, from);
            return;
        }
        let Ok(sock) = UdpSock::bind("0.0.0.0:0".parse().unwrap(), 8 << 20, 16 << 20) else { return };
        if sock.connect(SocketAddr::new(from.ip(), peer_port)).is_err() {
            return;
        }
        let port = match sock.local_addr() {
            Ok(a) => a.port(),
            Err(_) => return,
        };
        let e = crypto::ephemeral();
        let pub_r = e.public;
        let Some(shared) = crypto::dh(e, &eph_b) else { return };
        let mut tr = eph_b.clone();
        tr.extend_from_slice(&pub_r);
        tr.extend_from_slice(&session.to_le_bytes());
        let keys = crypto::session_keys(&pskb, &shared, &tr);
        let macr = crypto::mac(&pskb, &[b"acc", &session.to_le_bytes(), &pub_r, &eph_b, &port.to_le_bytes()]);
        let acc = Msg::SessAcc { session, port, eph: hex(&pub_r), mac: hex(&macr) };
        self.inner.lock().unwrap().sessions.insert(session, (port, hex(&pub_r), hex(&macr)));
        self.send_msg(&acc, from);
        let mut ch = Channel::new(sock, keys.resp_to_init, keys.init_to_resp, session);
        let _ = ch.send_plain(T_BEAT, &[0, 0, 0, 0, 0, 0]);
        let node = self.clone();
        let _ = std::thread::Builder::new().name("dd-recv".into()).spawn(move || node.recv_session(ch, peer, name));
    }

    fn connect_peer(&self, peer: &str) -> Result<Channel, String> {
        let (me, psk, addr) = {
            let g = self.inner.lock().unwrap();
            let p = g.peers.get(peer).ok_or("устройство не сопряжено")?;
            let addr = g.seen.get(peer).map(|s| s.addr).or(p.addr).ok_or("адрес устройства неизвестен")?;
            (g.me.id.clone(), p.psk.clone(), addr)
        };
        let pskb = unhex(&psk).ok_or("повреждён ключ")?;
        let sock = UdpSock::bind("0.0.0.0:0".parse().unwrap(), 8 << 20, 8 << 20).map_err(|e| e.to_string())?;
        sock.set_recv_timeout(Some(Duration::from_millis(100))).map_err(|e| e.to_string())?;
        let my_port = sock.local_addr().map_err(|e| e.to_string())?.port();
        let session = u32::from_le_bytes(crypto::random_bytes::<4>());
        let e = crypto::ephemeral();
        let pub_i = e.public;
        let nonce = crypto::random_bytes::<16>();
        let mac = crypto::mac(&pskb, &[b"req", &session.to_le_bytes(), &my_port.to_le_bytes(), &pub_i, &nonce, me.as_bytes()]);
        let req = Msg::SessReq { from: me, session, port: my_port, eph: hex(&pub_i), nonce: hex(&nonce), mac: hex(&mac) };
        let (tx, rx) = std::sync::mpsc::channel();
        self.inner.lock().unwrap().pending_session.insert(session, tx);
        let mut b = MAGIC.to_vec();
        b.extend_from_slice(&serde_json::to_vec(&req).unwrap_or_default());
        let mut acc = None;
        for _ in 0..16 {
            // Запрос уходит с сокета службы: ответ придёт на него же и пройдёт через брандмауэр.
            let _ = self.svc.send_to(&b, addr);
            if let Ok(m) = rx.recv_timeout(Duration::from_millis(250)) {
                acc = Some(m);
                break;
            }
        }
        self.inner.lock().unwrap().pending_session.remove(&session);
        let Some(Msg::SessAcc { port, eph, mac, .. }) = acc else {
            return Err("устройство не отвечает".into());
        };
        let eph_b = unhex(&eph).ok_or("плохой ответ")?;
        let macb = unhex(&mac).ok_or("плохой ответ")?;
        if !crypto::mac_ok(&pskb, &[b"acc", &session.to_le_bytes(), &eph_b, &pub_i, &port.to_le_bytes()], &macb) {
            return Err("устройство не подтвердило ключ".into());
        }
        let shared = crypto::dh(e, &eph_b).ok_or("ошибка обмена ключами")?;
        let mut tr = pub_i.to_vec();
        tr.extend_from_slice(&eph_b);
        tr.extend_from_slice(&session.to_le_bytes());
        let keys = crypto::session_keys(&pskb, &shared, &tr);
        sock.connect(SocketAddr::new(addr.ip(), port)).map_err(|e| e.to_string())?;
        let mut ch = Channel::new(sock, keys.init_to_resp, keys.resp_to_init, session);
        let _ = ch.send_plain(T_BEAT, &[0, 0, 0, 0, 0, 0]);
        Ok(ch)
    }

    // ---------- получатель ----------

    fn recv_session(self: Arc<Self>, mut ch: Channel, peer: String, peer_name: String) {
        // Предложение определяет задание; до него общее состояние временное.
        let sh = Arc::new(Shared::default());
        let node = self.clone();
        let mut job_id: Option<String> = None;
        let mut offer_key: Option<String> = None;
        let sh2 = sh.clone();
        let (peer2, name2) = (peer.clone(), peer_name.clone());
        let session = ch.session;
        let out = engine::run_receiver(&mut ch, &sh, |o| {
            if job_id.is_none() {
                job_id = Some(o.job.clone());
                node.register_recv_job(o, &sh2, &peer2, &name2);
            }
            node.decide(o, &peer2, &name2, &mut offer_key)
        });
        let mut g = self.inner.lock().unwrap();
        g.sessions.remove(&session);
        if let Some(k) = offer_key {
            g.pending.remove(&k);
            g.decisions.remove(&k);
        }
        let Some(id) = job_id else { return };
        let Some(j) = g.jobs.get_mut(&id) else { return };
        j.finished_at = None;
        match out {
            Outcome::Completed => {
                j.state = "done".into();
                j.finished_at = Some(now_unix());
                g.accepted.remove(&id);
                self.save_accepted(&g);
            }
            Outcome::LinkLost => {
                j.state = "interrupted".into();
                j.message = "связь потеряна, ждём отправителя".into();
            }
            Outcome::Cancelled(r) => {
                j.state = "cancelled".into();
                j.message = r;
                j.finished_at = Some(now_unix());
                g.accepted.remove(&id);
                self.save_accepted(&g);
            }
            Outcome::Declined(r) => {
                j.state = "declined".into();
                j.message = r;
                j.finished_at = Some(now_unix());
            }
            Outcome::Failed(r) => {
                j.state = "failed".into();
                j.message = r;
                j.finished_at = Some(now_unix());
            }
        }
    }

    fn register_recv_job(&self, o: &OfferInfo, sh: &Arc<Shared>, peer: &str, peer_name: &str) {
        let mut g = self.inner.lock().unwrap();
        let (id, name) = (o.job.clone(), o.name.clone());
        if !g.jobs.contains_key(&id) {
            g.order.push(id.clone());
        }
        let keep_start = g.jobs.get(&id).map(|j| j.active_secs).unwrap_or(0.0);
        let mut j = new_job(id.clone(), "recv", peer, peer_name, name, sh.clone());
        j.state = "offer".into();
        j.active_secs = keep_start;
        j.from_llm = o.from_llm;
        g.jobs.insert(id, j);
    }

    fn decide(&self, o: &OfferInfo, peer: &str, peer_name: &str, key: &mut Option<String>) -> Option<Decision> {
        let mut g = self.inner.lock().unwrap();
        // Возобновление: уже принятое задание не спрашиваем заново.
        if let Some(a) = g.accepted.get(&o.job).cloned() {
            if let Some(j) = g.jobs.get_mut(&o.job) {
                j.state = "running".into();
                j.dest = Some(short_home(&a.dest));
            }
            return Some(Decision::Accept { dest: a.dest, exclude: a.exclude });
        }
        if let Some(k) = key.clone() {
            let d = g.decisions.remove(&k)?;
            g.pending.remove(&k);
            *key = None;
            if let Decision::Accept { dest, exclude } = &d {
                if let Err(reason) = check_dest(o, dest, exclude) {
                    return Some(Decision::Decline(reason));
                }
                g.accepted.insert(o.job.clone(), AcceptedFile { job: o.job.clone(), dest: dest.clone(), exclude: exclude.clone() });
                self.save_accepted(&g);
                if let Some(j) = g.jobs.get_mut(&o.job) {
                    j.state = "running".into();
                    j.dest = Some(short_home(dest));
                }
            }
            return Some(d);
        }
        let dest = o.dest.as_deref().map(expand_dest).unwrap_or_else(|| {
            let d = default_roots();
            d.iter().find(|p| p.ends_with("Downloads")).cloned().unwrap_or_else(|| d[0].clone())
        });
        let auto = g.peers.get(peer).is_some_and(|p| p.auto_accept);
        let check = check_dest(o, &dest, &[]);
        // Замену файлов на месте человек подтверждает всегда.
        if auto && dest_allowed(&dest) && !o.in_place {
            if let Err(reason) = check {
                return Some(Decision::Decline(reason));
            }
            g.accepted.insert(o.job.clone(), AcceptedFile { job: o.job.clone(), dest: dest.clone(), exclude: vec![] });
            self.save_accepted(&g);
            if let Some(j) = g.jobs.get_mut(&o.job) {
                j.state = "running".into();
                j.dest = Some(short_home(&dest));
            }
            return Some(Decision::Accept { dest, exclude: vec![] });
        }
        let k = hex(&crypto::random_bytes::<6>());
        let conflict = o.items.iter().filter(|i| dest.join(&i.name).exists()).map(|i| i.name.clone()).collect();
        g.pending.insert(
            k.clone(),
            PendingOffer {
                offer: k.clone(),
                job: o.job.clone(),
                peer: peer.into(),
                peer_name: peer_name.into(),
                name: o.name.clone(),
                items: o.items.clone(),
                total: o.total,
                dest: short_home(&dest),
                overwrite: o.overwrite,
                from_llm: o.from_llm,
                in_place: o.in_place,
                conflict,
            },
        );
        *key = Some(k);
        None
    }

    pub fn answer_offer(&self, offer: &str, accept: bool, dest: Option<&str>, exclude: Vec<String>) -> Result<Value, String> {
        let mut g = self.inner.lock().unwrap();
        let p = g.pending.get(offer).cloned().ok_or("нет такого предложения")?;
        let d = if accept {
            Decision::Accept { dest: expand_dest(dest.unwrap_or(&p.dest)), exclude }
        } else {
            Decision::Decline("получатель отказался".into())
        };
        g.decisions.insert(offer.to_string(), d);
        Ok(json!({"ok": true}))
    }

    // ---------- отправитель ----------

    pub fn send(
        self: &Arc<Self>,
        peer: &str,
        paths: Vec<PathBuf>,
        dest: Option<String>,
        overwrite: bool,
        from_llm: bool,
        in_place: bool,
    ) -> Result<Value, String> {
        if paths.is_empty() {
            return Err("ничего не выбрано".into());
        }
        for p in &paths {
            if fs::symlink_metadata(p).is_err() {
                return Err(format!("нет такого пути: {}", p.display()));
            }
        }
        let peer_id = {
            let g = self.inner.lock().unwrap();
            g.peers.values().find(|p| p.id == peer || p.name == peer).map(|p| p.id.clone()).ok_or_else(|| format!("устройство не сопряжено: {peer}"))?
        };
        let id = hex(&crypto::random_bytes::<8>());
        let name = if paths.len() == 1 {
            paths[0].file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "файлы".into())
        } else {
            format!("{} и ещё {}", paths[0].file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default(), paths.len() - 1)
        };
        let jf = JobFile {
            job: id.clone(),
            peer: peer_id,
            paths,
            dest,
            overwrite,
            from_llm,
            name,
            finished: false,
            in_place,
            seq: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64),
        };
        save_job_file(&jf);
        self.launch_send(jf);
        Ok(json!({"job": id}))
    }

    fn resume_saved_jobs(self: &Arc<Self>) {
        let Ok(rd) = fs::read_dir(data_dir().join("jobs")) else { return };
        let mut saved: Vec<JobFile> = rd.flatten().filter_map(|e| fs::read(e.path()).ok().and_then(|d| serde_json::from_slice(&d).ok())).collect();
        saved.sort_by_key(|j: &JobFile| j.seq);
        for jf in saved.into_iter().filter(|j| !j.finished) {
            self.launch_send(jf);
        }
    }

    fn launch_send(self: &Arc<Self>, jf: JobFile) {
        let sh = Arc::new(Shared::default());
        {
            let mut g = self.inner.lock().unwrap();
            let pname = g.peers.get(&jf.peer).map(|p| p.name.clone()).unwrap_or_default();
            let mut j = new_job(jf.job.clone(), "send", &jf.peer, &pname, jf.name.clone(), sh.clone());
            j.from_llm = jf.from_llm;
            j.dest = jf.dest.clone();
            let busy = g.send_queue.iter().any(|q| q.1 == jf.peer);
            j.state = if busy { "queued" } else { "connecting" }.into();
            sh.total.store(jf.paths.iter().map(|p| tree_size(p)).sum(), Relaxed);
            g.send_queue.push((jf.job.clone(), jf.peer.clone()));
            g.order.push(jf.job.clone());
            g.jobs.insert(jf.job.clone(), j);
        }
        let node = self.clone();
        let _ = std::thread::Builder::new().name("dd-send".into()).spawn(move || node.send_job(jf, sh));
    }

    fn set_state(&self, id: &str, st: &str, msg: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some(j) = g.jobs.get_mut(id) {
            j.state = st.into();
            j.message = msg.into();
            if matches!(st, "done" | "failed" | "cancelled" | "declined") {
                j.finished_at = Some(now_unix());
            }
        }
    }

    /// Ждёт, пока до задания дойдёт очередь к его устройству.
    fn wait_turn(&self, jf: &JobFile, sh: &Shared) {
        while !sh.cancel.load(Relaxed) {
            {
                let mut g = self.inner.lock().unwrap();
                if g.send_queue.iter().find(|q| q.1 == jf.peer).is_some_and(|q| q.0 == jf.job) {
                    if let Some(j) = g.jobs.get_mut(&jf.job) {
                        j.state = "connecting".into();
                    }
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn send_job(self: Arc<Self>, mut jf: JobFile, sh: Arc<Shared>) {
        self.wait_turn(&jf, &sh);
        let plan = SendPlan {
            job: jf.job.clone(),
            name: jf.name.clone(),
            paths: jf.paths.clone(),
            dest: jf.dest.clone(),
            overwrite: jf.overwrite,
            from_llm: jf.from_llm,
            in_place: jf.in_place,
        };
        let mut attempt = 0u32;
        loop {
            if sh.cancel.load(Relaxed) {
                self.set_state(&jf.job, "cancelled", "отменено");
                break;
            }
            match self.connect_peer(&jf.peer) {
                Ok(mut ch) => {
                    attempt = 0;
                    self.set_state(&jf.job, "running", "");
                    match engine::run_sender(&mut ch, &plan, &sh) {
                        Outcome::Completed => {
                            self.set_state(&jf.job, "done", "");
                            break;
                        }
                        Outcome::LinkLost => {
                            self.set_state(&jf.job, "reconnecting", "связь потеряна, ищу устройство");
                        }
                        Outcome::Cancelled(r) => {
                            self.set_state(&jf.job, "cancelled", &r);
                            break;
                        }
                        Outcome::Declined(r) => {
                            self.set_state(&jf.job, "declined", &r);
                            break;
                        }
                        Outcome::Failed(r) => {
                            self.set_state(&jf.job, "failed", &r);
                            break;
                        }
                    }
                }
                Err(e) => {
                    self.set_state(&jf.job, "reconnecting", &e);
                }
            }
            attempt += 1;
            let wait = 1u64 << attempt.min(3);
            for _ in 0..wait * 10 {
                if sh.cancel.load(Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        jf.finished = true;
        self.inner.lock().unwrap().send_queue.retain(|q| q.0 != jf.job);
        let _ = fs::remove_file(job_path(&jf.job));
    }

    // ---------- состояние ----------

    fn tick_loop(self: Arc<Self>) {
        loop {
            std::thread::sleep(Duration::from_millis(500));
            let mut g = self.inner.lock().unwrap();
            let now = Instant::now();
            let mut any_active = false;
            for j in g.jobs.values_mut() {
                let b = j.shared.moved.load(Relaxed);
                let dt = now.duration_since(j.last_tick).as_secs_f64();
                j.last_tick = now;
                let running = j.state == "running" && !j.shared.pause.load(Relaxed);
                if matches!(j.state.as_str(), "running" | "reconnecting" | "connecting" | "interrupted") {
                    any_active = any_active || j.state != "interrupted";
                }
                if running {
                    let phase_transfer = *j.shared.phase.lock().unwrap() == "transfer";
                    if phase_transfer {
                        j.bytes_at_start.get_or_insert(0);
                        j.active_secs += dt;
                        j.speed = b.saturating_sub(j.last_bytes) as f64 / dt.max(1e-3);
                        j.samples.push_back(j.speed);
                    } else {
                        j.speed = 0.0;
                    }
                } else {
                    j.speed = 0.0;
                    if matches!(j.state.as_str(), "reconnecting" | "interrupted") {
                        j.samples.push_back(0.0);
                    }
                }
                while j.samples.len() > 120 {
                    j.samples.pop_front();
                }
                j.last_bytes = b;
            }
            if any_active && g.inhibitor.is_none() {
                g.inhibitor = start_inhibitor();
            } else if !any_active {
                if let Some(mut c) = g.inhibitor.take() {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
            if g.pair.as_ref().is_some_and(|p| p.started.elapsed() > PAIR_CODE_TTL && p.status != "paired") {
                g.pair = None;
            }
        }
    }

    pub fn status(&self) -> Value {
        let g = self.inner.lock().unwrap();
        let online = |id: &str| g.seen.get(id).is_some_and(|s| s.at.elapsed() < ONLINE_FOR);
        let mut devices: Vec<Value> = vec![];
        for p in g.peers.values() {
            let s = g.seen.get(&p.id);
            devices.push(json!({
                "id": p.id, "name": s.map(|x| x.name.clone()).unwrap_or_else(|| p.name.clone()),
                "paired": true, "online": online(&p.id), "auto_accept": p.auto_accept,
                "addr": s.map(|x| x.addr.to_string()).or(p.addr.map(|a| a.to_string())),
                "roots": s.map(|x| x.roots.clone()).unwrap_or_default(),
            }));
        }
        for (id, s) in &g.seen {
            if !g.peers.contains_key(id) && s.at.elapsed() < ONLINE_FOR {
                devices.push(json!({"id": id, "name": s.name, "paired": false, "online": true, "addr": s.addr.to_string(), "roots": []}));
            }
        }
        devices.sort_by_key(|d| d["name"].as_str().unwrap_or("").to_string());
        let jobs: Vec<Value> = g.order.iter().rev().filter_map(|id| g.jobs.get(id)).map(job_json).collect();
        let pending: Vec<&PendingOffer> = g.pending.values().collect();
        let pair = g.pair.as_ref().map(|p| json!({"peer": p.peer, "peer_name": p.peer_name, "code": p.code, "status": p.status, "secs_left": PAIR_CODE_TTL.saturating_sub(p.started.elapsed()).as_secs()}));
        json!({
            "me": {"id": g.me.id, "name": g.me.name, "roots": this_roots(), "os": std::env::consts::OS},
            "devices": devices, "jobs": jobs, "offers": pending, "pair": pair,
        })
    }

    pub fn job_control(&self, id: &str, action: &str) -> Result<Value, String> {
        let mut g = self.inner.lock().unwrap();
        let jid = if id == "last" { g.order.last().cloned().ok_or("заданий нет")? } else { id.to_string() };
        let j = g.jobs.get_mut(&jid).ok_or("нет такого задания")?;
        match action {
            "pause" => j.shared.pause.store(true, Relaxed),
            "resume" => j.shared.pause.store(false, Relaxed),
            "cancel" => j.shared.cancel.store(true, Relaxed),
            _ => return Err("неизвестное действие".into()),
        }
        Ok(json!({"ok": true, "job": jid}))
    }

    /// Прогресс по файлам: сколько блоков файла принято.
    pub fn job_files(&self, id: &str, offset: usize, limit: usize, filter: &str) -> Result<Value, String> {
        let (m, blocks) = {
            let g = self.inner.lock().unwrap();
            let j = if id == "last" { g.order.last().and_then(|i| g.jobs.get(i)) } else { g.jobs.get(id) }.ok_or("нет такого задания")?;
            let m = j.shared.manifest.lock().unwrap().clone();
            let b = j.shared.blocks.lock().unwrap().clone();
            (m, b)
        };
        let Some(m): Option<Arc<Manifest>> = m else {
            return Ok(json!({"files": [], "total": 0}));
        };
        let mut out = vec![];
        let mut matched = 0usize;
        for e in m.files() {
            let (first, last) =
                if e.size == 0 { (0usize, 0usize) } else { ((e.offset / BLOCK as u64) as usize, ((e.offset + e.size - 1) / BLOCK as u64) as usize) };
            let span = if e.size == 0 { 0 } else { last - first + 1 };
            let got = if e.size == 0 { 0 } else { blocks.get(first..=last).map(|r| r.iter().filter(|x| **x).count()).unwrap_or(0) };
            let frac = if span == 0 { 1.0 } else { got as f64 / span as f64 };
            let state = if frac >= 1.0 {
                "done"
            } else if frac > 0.0 {
                "active"
            } else {
                "queue"
            };
            if filter != "all" && filter != state {
                continue;
            }
            if matched >= offset && out.len() < limit {
                out.push(json!({"path": e.path, "size": e.size, "progress": frac, "state": state}));
            }
            matched += 1;
        }
        Ok(json!({"files": out, "total": matched}))
    }
}

fn new_job(id: String, role: &'static str, peer: &str, peer_name: &str, name: String, shared: Arc<Shared>) -> Job {
    Job {
        id,
        role,
        peer: peer.to_string(),
        peer_name: peer_name.to_string(),
        name,
        shared,
        state: "connecting".into(),
        message: String::new(),
        from_llm: false,
        dest: None,
        started: Instant::now(),
        active_secs: 0.0,
        bytes_at_start: None,
        last_bytes: 0,
        last_tick: Instant::now(),
        speed: 0.0,
        samples: VecDeque::new(),
        finished_at: None,
    }
}

fn job_json(j: &Job) -> Value {
    let s = &j.shared;
    let total = s.total.load(Relaxed);
    let bytes = if j.state == "done" { total } else { s.bytes.load(Relaxed) };
    let moved = s.moved.load(Relaxed).saturating_sub(j.bytes_at_start.unwrap_or(0));
    let avg = if j.active_secs > 0.5 { moved as f64 / j.active_secs } else { 0.0 };
    let eta = if avg > 1.0 && total > bytes { Some((total - bytes) as f64 / avg) } else { None };
    let (nb, gb) = {
        let b = s.blocks.lock().unwrap();
        (b.len(), b.iter().filter(|x| **x).count())
    };
    let m = s.manifest.lock().unwrap().clone();
    let files_total = m.as_ref().map(|m| m.entries.iter().filter(|e| e.kind == 'f').count()).unwrap_or(0);
    json!({
        "id": j.id, "role": j.role, "peer": j.peer, "peer_name": j.peer_name, "name": j.name,
        "state": j.state, "message": j.message, "phase": *s.phase.lock().unwrap(),
        "paused": s.pause.load(Relaxed), "from_llm": j.from_llm, "dest": j.dest,
        "bytes": bytes, "total": total, "speed": j.speed, "avg_speed": avg, "eta_secs": eta,
        "retx": s.retx.load(Relaxed), "sent_pkts": s.sent_pkts.load(Relaxed), "repaired": s.repaired.load(Relaxed),
        "rate": s.rate.load(Relaxed), "rtt_us": s.rtt_us.load(Relaxed),
        "phase_done": s.phase_done.load(Relaxed), "phase_total": s.phase_total.load(Relaxed),
        "root": *s.root.lock().unwrap(), "blocks_total": nb, "blocks_done": gb, "files_total": files_total,
        "active_secs": j.active_secs, "elapsed_secs": j.started.elapsed().as_secs_f64(), "finished_at": j.finished_at,
        "stalls": {"nodata": s.stall_nodata.load(Relaxed), "credit": s.stall_credit.load(Relaxed), "window": s.stall_window.load(Relaxed), "enobufs": s.stall_enobufs.load(Relaxed), "budget": s.stall_budget.load(Relaxed), "lost": s.stall_lost.load(Relaxed)},
        "samples": j.samples.iter().map(|x| *x as u64).collect::<Vec<_>>(),
    })
}

fn this_roots() -> Vec<Root> {
    default_roots().iter().map(|p| Root { path: short_home(p), free: engine::free_space(p).unwrap_or(0) }).collect()
}

fn job_path(id: &str) -> PathBuf {
    data_dir().join("jobs").join(format!("{id}.json"))
}

fn save_job_file(j: &JobFile) {
    if let Ok(d) = serde_json::to_vec_pretty(j) {
        let _ = write_private(&job_path(&j.job), &d);
    }
}

fn check_dest(o: &OfferInfo, dest: &Path, exclude: &[String]) -> Result<(), String> {
    let mut need: u64 = o.items.iter().filter(|i| !exclude.contains(&i.name)).map(|i| i.size).sum();
    if o.in_place {
        // Существующие файлы заменяются на месте, поэтому нужен только их прирост.
        let existing: u64 = o.items.iter().filter(|i| !exclude.contains(&i.name)).map(|i| tree_size(&dest.join(&i.name))).sum();
        need = need.saturating_sub(existing);
    }
    if let Some(free) = engine::free_space(dest) {
        // Если это возобновление, часть уже записана, но запас лучше проверить на всё задание.
        if free < need {
            return Err(format!("мало места: нужно {:.1} ГБ, свободно {:.1} ГБ", need as f64 / 1e9, free as f64 / 1e9));
        }
    }
    if !o.overwrite && !o.in_place {
        for i in o.items.iter().filter(|i| !exclude.contains(&i.name)) {
            if fs::symlink_metadata(dest.join(&i.name)).is_ok() {
                return Err(format!("{} уже есть в {}, перезапись не разрешена", i.name, short_home(dest)));
            }
        }
    }
    if fs::create_dir_all(dest).is_err() {
        return Err(format!("не удалось создать {}", dest.display()));
    }
    Ok(())
}

fn tree_size(p: &Path) -> u64 {
    let Ok(md) = fs::symlink_metadata(p) else { return 0 };
    if md.is_dir() {
        fs::read_dir(p).map(|rd| rd.flatten().map(|e| tree_size(&e.path())).sum()).unwrap_or(0)
    } else {
        md.len()
    }
}

fn start_inhibitor() -> Option<std::process::Child> {
    use std::process::{Command, Stdio};
    let pid = std::process::id().to_string();
    let mut c = if cfg!(target_os = "macos") {
        let mut c = Command::new("/usr/bin/caffeinate");
        c.args(["-i", "-w", &pid]);
        c
    } else {
        let mut c = Command::new("systemd-inhibit");
        c.args(["--what=idle", "--who=DeckDrop", "--why=Передача файлов", "--mode=block", "sleep", "infinity"]);
        c
    };
    c.stdout(Stdio::null()).stderr(Stdio::null()).stdin(Stdio::null()).spawn().ok()
}
