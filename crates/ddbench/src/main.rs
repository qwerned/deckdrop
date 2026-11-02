//! Замеры способов передачи. Запускается парой: приёмник на одном устройстве, отправитель на другом.
use ddnet::{is_timeout, is_transient_send_error, RecvBatch, UdpSock};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const END_MAGIC: &[u8] = b"DDBENCH-END!";

struct Args(HashMap<String, String>);

impl Args {
    fn parse(a: &[String]) -> Self {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < a.len() {
            if let Some(k) = a[i].strip_prefix("--") {
                let v = a.get(i + 1).filter(|x| !x.starts_with("--")).cloned();
                if v.is_some() {
                    i += 1;
                }
                m.insert(k.to_string(), v.unwrap_or_else(|| "1".into()));
            }
            i += 1;
        }
        Self(m)
    }
    fn s(&self, k: &str, d: &str) -> String {
        self.0.get(k).cloned().unwrap_or_else(|| d.to_string())
    }
    fn n(&self, k: &str, d: f64) -> f64 {
        self.0.get(k).and_then(|v| v.parse().ok()).unwrap_or(d)
    }
    fn u(&self, k: &str, d: usize) -> usize {
        self.n(k, d as f64) as usize
    }
}

fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    u.ru_utime.tv_sec as f64 + u.ru_utime.tv_usec as f64 / 1e6 + u.ru_stime.tv_sec as f64 + u.ru_stime.tv_usec as f64 / 1e6
}

fn mb(b: u64, s: f64) -> f64 {
    b as f64 / 1e6 / s.max(1e-6)
}

fn progress_printer(label: &'static str, ctr: Arc<AtomicU64>, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut last = 0;
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            let v = ctr.load(Ordering::Relaxed);
            println!("{label}: {:.1} МБ/с", mb(v - last, 1.0));
            last = v;
        }
    })
}

// ---------- сырой UDP ----------

fn udp_recv(a: &Args) {
    let base = a.u("port", 52800) as u16;
    let threads = a.u("threads", 1);
    let batch = a.u("batch", 64);
    let gro = a.0.contains_key("gro");
    let bytes = Arc::new(AtomicU64::new(0));
    let pkts = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let pr = progress_printer("приём", bytes.clone(), stop.clone());
    let start = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let end = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let mut hs = vec![];
    for t in 0..threads {
        let (bytes, pkts, done, start, end) = (bytes.clone(), pkts.clone(), done.clone(), start.clone(), end.clone());
        hs.push(std::thread::spawn(move || {
            let s = UdpSock::bind(format!("0.0.0.0:{}", base + t as u16).parse().unwrap(), 1 << 20, 64 << 20).unwrap();
            if t == 0 {
                println!("буфер приёма {} байт", s.buffer_sizes().1);
            }
            #[cfg(target_os = "linux")]
            if gro {
                s.enable_gro().unwrap();
            }
            let _ = gro;
            s.set_recv_timeout(Some(Duration::from_millis(200))).unwrap();
            let pkt_size = if gro { 65535 } else { 2048 };
            let mut rb = RecvBatch::new(batch.max(1), pkt_size);
            while !done.load(Ordering::Relaxed) {
                match s.recv_batch(&mut rb) {
                    Ok(n) => {
                        let mut got = 0u64;
                        for i in 0..n {
                            let p = rb.pkt(i);
                            if p.starts_with(END_MAGIC) {
                                done.store(true, Ordering::Relaxed);
                                let mut e = end.lock().unwrap();
                                e.get_or_insert_with(Instant::now);
                            } else {
                                got += p.len() as u64;
                            }
                        }
                        start.lock().unwrap().get_or_insert_with(Instant::now);
                        bytes.fetch_add(got, Ordering::Relaxed);
                        pkts.fetch_add(n as u64, Ordering::Relaxed);
                    }
                    Err(e) if is_timeout(&e) => {}
                    Err(e) => panic!("{e}"),
                }
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    pr.join().unwrap();
    let st = start.lock().unwrap().unwrap();
    let dur = end.lock().unwrap().unwrap().duration_since(st).as_secs_f64();
    let b = bytes.load(Ordering::Relaxed);
    println!("ИТОГ приём: {:.1} МБ/с за {:.1} с, вызовов-пакетов {}, ЦП {:.0} %", mb(b, dur), dur, pkts.load(Ordering::Relaxed), cpu_secs() / dur * 100.0);
}

fn udp_send(a: &Args) {
    let to = a.s("to", "127.0.0.1");
    let base = a.u("port", 52800) as u16;
    let threads = a.u("threads", 1);
    let batch = a.u("batch", 64);
    let size = a.u("size", 1440);
    let secs = a.n("secs", 10.0);
    let rate = a.n("rate", 0.0) * 1e6 / threads as f64;
    let gso = a.0.contains_key("gso");
    let bytes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let pr = progress_printer("отправка", bytes.clone(), stop.clone());
    let enobufs = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut hs = vec![];
    for t in 0..threads {
        let (to, bytes, enobufs) = (to.clone(), bytes.clone(), enobufs.clone());
        hs.push(std::thread::spawn(move || {
            let s = UdpSock::bind("0.0.0.0:0".parse().unwrap(), 8 << 20, 1 << 20).unwrap();
            s.connect(format!("{to}:{}", base + t as u16).parse().unwrap()).unwrap();
            let data = vec![0xABu8; size * batch.max(1)];
            let pkts: Vec<&[u8]> = data.chunks(size).collect();
            let start = Instant::now();
            let mut sent = 0u64;
            while start.elapsed().as_secs_f64() < secs {
                if rate > 0.0 {
                    let allowed = start.elapsed().as_secs_f64() * rate;
                    if sent as f64 > allowed {
                        std::thread::sleep(Duration::from_micros(100));
                        continue;
                    }
                }
                #[cfg(target_os = "linux")]
                let r = if gso {
                    s.send_gso(&data, size as u16).map(|n| n / size)
                } else if batch <= 1 {
                    s.send(pkts[0]).map(|_| 1)
                } else {
                    s.send_batch(&pkts)
                };
                #[cfg(not(target_os = "linux"))]
                let r = {
                    let _ = gso;
                    if batch <= 1 {
                        s.send(pkts[0]).map(|_| 1)
                    } else {
                        s.send_batch(&pkts)
                    }
                };
                match r {
                    Ok(n) => {
                        sent += (n * size) as u64;
                        bytes.fetch_add((n * size) as u64, Ordering::Relaxed);
                    }
                    Err(e) if is_transient_send_error(&e) => {
                        enobufs.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    Err(e) => panic!("{e}"),
                }
            }
            for _ in 0..20 {
                let _ = s.send(END_MAGIC);
                std::thread::sleep(Duration::from_millis(5));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    pr.join().unwrap();
    let dur = t0.elapsed().as_secs_f64();
    println!(
        "ИТОГ отправка: {:.1} МБ/с за {:.1} с, ENOBUFS {}, ЦП {:.0} %",
        mb(bytes.load(Ordering::Relaxed), dur),
        dur,
        enobufs.load(Ordering::Relaxed),
        cpu_secs() / dur * 100.0
    );
}

// ---------- TCP ----------

fn tcp_recv(a: &Args) {
    let port = a.u("port", 52810);
    let l = TcpListener::bind(format!("0.0.0.0:{port}")).unwrap();
    let bytes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let pr = progress_printer("приём TCP", bytes.clone(), stop.clone());
    let mut hs: Vec<std::thread::JoinHandle<()>> = vec![];
    let streams = a.u("streams", 1);
    let mut t0 = None;
    for _ in 0..streams {
        let (mut c, _) = l.accept().unwrap();
        t0.get_or_insert_with(Instant::now);
        let bytes = bytes.clone();
        hs.push(std::thread::spawn(move || {
            let mut b = vec![0u8; 1 << 20];
            loop {
                match c.read(&mut b) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        bytes.fetch_add(n as u64, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    pr.join().unwrap();
    let dur = t0.unwrap().elapsed().as_secs_f64();
    println!("ИТОГ приём TCP x{streams}: {:.1} МБ/с за {:.1} с, ЦП {:.0} %", mb(bytes.load(Ordering::Relaxed), dur), dur, cpu_secs() / dur * 100.0);
}

fn tcp_send(a: &Args) {
    let to = a.s("to", "127.0.0.1");
    let port = a.u("port", 52810);
    let streams = a.u("streams", 1);
    let secs = a.n("secs", 10.0);
    let mut hs = vec![];
    for _ in 0..streams {
        let to = to.clone();
        hs.push(std::thread::spawn(move || {
            let mut c = TcpStream::connect(format!("{to}:{port}")).unwrap();
            c.set_nodelay(true).ok();
            let b = vec![0xCDu8; 1 << 20];
            let t0 = Instant::now();
            while t0.elapsed().as_secs_f64() < secs {
                c.write_all(&b).unwrap();
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    println!("ИТОГ отправка TCP закончена, ЦП {:.1} с", cpu_secs());
}

// ---------- QUIC (quinn) ----------

fn quic_transport(cc: &str) -> quinn::TransportConfig {
    let mut t = quinn::TransportConfig::default();
    t.stream_receive_window((64u32 << 20).into());
    t.receive_window((128u32 << 20).into());
    t.send_window(128 << 20);
    t.initial_mtu(1452);
    t.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
    match cc {
        "bbr" => {
            t.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
        }
        "newreno" => {
            t.congestion_controller_factory(Arc::new(quinn::congestion::NewRenoConfig::default()));
        }
        _ => {
            t.congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
        }
    }
    t
}

#[derive(Debug)]
struct NoVerify;
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
    }
}

#[tokio::main]
async fn quic_recv(a: &Args) {
    let port = a.u("port", 52820);
    let cc = a.s("cc", "cubic");
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["deckdrop".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let mut sc = quinn::ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();
    sc.transport_config(Arc::new(quic_transport(&cc)));
    let ep = quinn::Endpoint::server(sc, format!("0.0.0.0:{port}").parse().unwrap()).unwrap();
    let conn = ep.accept().await.unwrap().await.unwrap();
    let bytes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let pr = progress_printer("приём QUIC", bytes.clone(), stop.clone());
    let t0 = Instant::now();
    let mut s = conn.accept_uni().await.unwrap();
    let mut b = vec![0u8; 1 << 20];
    while let Ok(Some(n)) = s.read(&mut b).await {
        bytes.fetch_add(n as u64, Ordering::Relaxed);
    }
    let dur = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    pr.join().unwrap();
    let st = conn.stats();
    println!(
        "ИТОГ приём QUIC: {:.1} МБ/с за {:.1} с, ЦП {:.0} %, rtt {:?}",
        mb(bytes.load(Ordering::Relaxed), dur),
        dur,
        cpu_secs() / dur * 100.0,
        st.path.rtt
    );
    conn.close(0u32.into(), b"ok");
    ep.wait_idle().await;
}

#[tokio::main]
async fn quic_send(a: &Args) {
    let to = a.s("to", "127.0.0.1");
    let port = a.u("port", 52820);
    let secs = a.n("secs", 10.0);
    let cc = a.s("cc", "cubic");
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut ep = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    let rc = rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(Arc::new(NoVerify)).with_no_client_auth();
    let mut cc_cfg = quinn::ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(rc).unwrap()));
    cc_cfg.transport_config(Arc::new(quic_transport(&cc)));
    ep.set_default_client_config(cc_cfg);
    let addr: SocketAddr = format!("{to}:{port}").parse().unwrap();
    let conn = ep.connect(addr, "deckdrop").unwrap().await.unwrap();
    let mut s = conn.open_uni().await.unwrap();
    let b = vec![0xEFu8; 1 << 20];
    let t0 = Instant::now();
    let mut sent = 0u64;
    let mut last = Instant::now();
    let mut last_b = 0u64;
    while t0.elapsed().as_secs_f64() < secs {
        s.write_all(&b).await.unwrap();
        sent += b.len() as u64;
        if last.elapsed().as_secs() >= 1 {
            println!(
                "отправка QUIC: {:.1} МБ/с, rtt {:?}, cwnd {}",
                mb(sent - last_b, last.elapsed().as_secs_f64()),
                conn.stats().path.rtt,
                conn.stats().path.cwnd
            );
            last = Instant::now();
            last_b = sent;
        }
    }
    s.finish().unwrap();
    conn.closed().await;
    let st = conn.stats();
    println!("ИТОГ отправка QUIC [{cc}]: потеряно пакетов {}, отправлено {}, ЦП {:.1} с", st.path.lost_packets, st.path.sent_packets, cpu_secs());
}

// ---------- криптография и хеш ----------

fn ring_bench(name: &str, alg: &'static ring::aead::Algorithm, keylen: usize) {
    use ring::aead::*;
    let key = LessSafeKey::new(UnboundKey::new(alg, &vec![7u8; keylen]).unwrap());
    let mut pkt = vec![0x5Au8; 1424];
    let n = 400_000u64;
    let t = Instant::now();
    for i in 0..n {
        let mut nonce = [0u8; 12];
        nonce[4..].copy_from_slice(&i.to_le_bytes());
        let _tag = key.seal_in_place_separate_tag(Nonce::assume_unique_for_key(nonce), Aad::from(&[1u8; 24]), &mut pkt).unwrap();
    }
    println!("ring {name}: {:.0} МБ/с на ядро", mb(n * 1424, t.elapsed().as_secs_f64()));
}

fn crypto_bench() {
    ring_bench("AES-128-GCM", &ring::aead::AES_128_GCM, 16);
    ring_bench("AES-256-GCM", &ring::aead::AES_256_GCM, 32);
    ring_bench("ChaCha20-Poly1305", &ring::aead::CHACHA20_POLY1305, 32);
    use aes_gcm::aead::{AeadInPlace, KeyInit};
    let mut pkt = vec![0x5Au8; 1400];
    let n = 200_000;
    let t = Instant::now();
    let c = aes_gcm::Aes256Gcm::new_from_slice(&[7u8; 32]).unwrap();
    for i in 0..n {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let _ = c.encrypt_in_place_detached((&nonce).into(), b"", &mut pkt).unwrap();
    }
    println!("AES-256-GCM: {:.0} МБ/с на ядро", mb(n as u64 * 1400, t.elapsed().as_secs_f64()));
    let t = Instant::now();
    let c = aes_gcm::Aes128Gcm::new_from_slice(&[7u8; 16]).unwrap();
    for i in 0..n {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let _ = c.encrypt_in_place_detached((&nonce).into(), b"", &mut pkt).unwrap();
    }
    println!("AES-128-GCM: {:.0} МБ/с на ядро", mb(n as u64 * 1400, t.elapsed().as_secs_f64()));
    let t = Instant::now();
    let c = chacha20poly1305::ChaCha20Poly1305::new_from_slice(&[7u8; 32]).unwrap();
    for i in 0..n {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let _ = c.encrypt_in_place_detached((&nonce).into(), b"", &mut pkt).unwrap();
    }
    println!("ChaCha20-Poly1305: {:.0} МБ/с на ядро", mb(n as u64 * 1400, t.elapsed().as_secs_f64()));
    let block = vec![0x11u8; 4 << 20];
    let t = Instant::now();
    for _ in 0..100 {
        let _ = blake3::hash(&block);
    }
    println!("BLAKE3 блоками 4 МиБ: {:.0} МБ/с на ядро", mb(400 << 20, t.elapsed().as_secs_f64()));
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cmd = argv.first().map(String::as_str).unwrap_or("");
    let a = Args::parse(argv.get(1..).unwrap_or(&[]));
    match cmd {
        "udp-recv" => udp_recv(&a),
        "udp-send" => udp_send(&a),
        "tcp-recv" => tcp_recv(&a),
        "tcp-send" => tcp_send(&a),
        "quic-recv" => quic_recv(&a),
        "quic-send" => quic_send(&a),
        "crypto" => crypto_bench(),
        _ => eprintln!("команды: udp-recv udp-send tcp-recv tcp-send quic-recv quic-send crypto"),
    }
}
