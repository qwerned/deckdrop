//! Отправитель и получатель одной передачи.
use crate::cc::Cc;
use crate::channel::*;
use crate::crypto::hex;
use crate::stream::*;
use ddnet::RecvBatch;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LINK_TIMEOUT: Duration = Duration::from_secs(6);
const BATCH: usize = 32;
const RECV_WINDOW_BLOCKS: u32 = 48;
const READAHEAD_BLOCKS: usize = 8;

/// Состояние передачи, видимое службе и интерфейсу.
#[derive(Default)]
pub struct Shared {
    pub pause: AtomicBool,
    pub cancel: AtomicBool,
    pub bytes: AtomicU64,
    /// Реально переданные байты, без уже имевшихся у получателя. По ним считается скорость.
    pub moved: AtomicU64,
    pub total: AtomicU64,
    pub retx: AtomicU64,
    pub sent_pkts: AtomicU64,
    pub rate: AtomicU64,
    pub rtt_us: AtomicU64,
    pub phase_done: AtomicU64,
    pub phase_total: AtomicU64,
    pub repaired: AtomicU64,
    /// Причины простоя отправителя: нет блока, нет разрешения получателя, окно, отказ буфера, нет бюджета.
    pub stall_nodata: AtomicU64,
    pub stall_lost: AtomicU64,
    pub stall_credit: AtomicU64,
    pub stall_window: AtomicU64,
    pub stall_enobufs: AtomicU64,
    pub stall_budget: AtomicU64,
    pub phase: Mutex<String>,
    pub root: Mutex<String>,
    pub blocks: Mutex<Vec<bool>>,
    pub manifest: Mutex<Option<Arc<Manifest>>>,
}

impl Shared {
    pub fn set_phase(&self, p: &str) {
        *self.phase.lock().unwrap() = p.to_string();
    }
}

#[derive(Debug)]
pub enum Outcome {
    Completed,
    LinkLost,
    Cancelled(String),
    Declined(String),
    Failed(String),
}

fn io_fail(e: impl std::fmt::Display) -> Outcome {
    Outcome::Failed(e.to_string())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn sim_env(name: &str) -> f64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(0.0)
}

// ========== отправитель ==========

struct Loaded {
    data: Vec<u8>,
    hash: [u8; 32],
}

fn reader_thread(manifest: Arc<Manifest>, sources: Arc<HashMap<String, PathBuf>>, work: Receiver<u32>, out: SyncSender<Result<(u32, Loaded), String>>) {
    let files = manifest.files();
    let mut cache = FileCache::new(32);
    while let Ok(b) = work.recv() {
        let len = manifest.block_len(b);
        let mut data = vec![0u8; len];
        let r = for_ranges(&files, b as u64 * BLOCK as u64, len, |e, off, pos, n| {
            let f = cache.get(&e.path, || {
                let p = sources.get(&e.path).ok_or_else(|| io::Error::other("нет источника"))?;
                File::open(p)
            });
            f.and_then(|f| f.read_exact_at(&mut data[pos..pos + n], off)).map_err(|err| format!("{}: {err}", e.path))
        });
        let msg = r.map(|_| {
            let hash = *blake3::hash(&data).as_bytes();
            (b, Loaded { data, hash })
        });
        if out.send(msg).is_err() {
            return;
        }
    }
}

pub struct SendPlan {
    pub job: String,
    pub name: String,
    pub paths: Vec<PathBuf>,
    pub dest: Option<String>,
    pub overwrite: bool,
    pub from_llm: bool,
    pub in_place: bool,
}

pub fn run_sender(ch: &mut Channel, plan: &SendPlan, sh: &Arc<Shared>) -> Outcome {
    let scan0 = match scan(&plan.paths, &[]) {
        Ok(s) => s,
        Err(e) => return Outcome::Failed(e.to_string()),
    };
    ch.push_ctrl(&Ctrl::Offer {
        job: plan.job.clone(),
        name: plan.name.clone(),
        items: scan0.items.clone(),
        total: scan0.manifest.total,
        dest: plan.dest.clone(),
        overwrite: plan.overwrite,
        from_llm: plan.from_llm,
        in_place: plan.in_place,
    });
    sh.set_phase("waiting_accept");
    sh.total.store(scan0.manifest.total, Relaxed);

    let mut rb = RecvBatch::new(BATCH, 2048);
    let _ = ch.sock.set_nonblocking(true);
    let mut inbox = vec![];
    let mut scan1 = None;
    let mut last_beat = Instant::now();
    let done_list;
    // согласование: предложение → согласие → манифест → список готовых блоков
    loop {
        if let Err(e) = ch.pump_ctrl() {
            return if is_net_down(&e) { Outcome::LinkLost } else { io_fail(e) };
        }
        drain_rx(ch, &mut rb, &mut inbox, |_, _, _| {});
        for m in inbox.drain(..) {
            match m {
                Ctrl::Accept { exclude, .. } => {
                    let s = if exclude.is_empty() {
                        scan0.clone_scan()
                    } else {
                        match scan(&plan.paths, &exclude) {
                            Ok(s) => s,
                            Err(e) => return Outcome::Failed(e.to_string()),
                        }
                    };
                    ch.push_ctrl(&Ctrl::Manifest { manifest: s.manifest.clone() });
                    sh.total.store(s.manifest.total, Relaxed);
                    *sh.blocks.lock().unwrap() = vec![false; s.manifest.blocks() as usize];
                    *sh.manifest.lock().unwrap() = Some(Arc::new(s.manifest.clone()));
                    scan1 = Some(s);
                }
                Ctrl::Decline { reason } => return Outcome::Declined(reason),
                Ctrl::Cancel { reason } => return Outcome::Cancelled(reason),
                Ctrl::Error { message } => return Outcome::Failed(message),
                Ctrl::Phase { phase, done, total } => {
                    sh.set_phase(&phase);
                    sh.phase_done.store(done, Relaxed);
                    sh.phase_total.store(total, Relaxed);
                }
                Ctrl::Ready { done } if scan1.is_some() => {
                    done_list = done;
                    return send_data(ch, scan1.take().unwrap(), done_list, sh, rb);
                }
                _ => {}
            }
        }
        if sh.cancel.load(Relaxed) {
            ch.push_ctrl(&Ctrl::Cancel { reason: "отменено отправителем".into() });
            let _ = flush_ctrl(ch, &mut rb);
            return Outcome::Cancelled("отменено".into());
        }
        if ch.last_rx.elapsed() > LINK_TIMEOUT {
            return Outcome::LinkLost;
        }
        if last_beat.elapsed() > Duration::from_millis(200) {
            last_beat = Instant::now();
            let _ = ch.send_plain(T_BEAT, &[0, 0, 0, 0, 0, 0]);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn is_net_down(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::ENETDOWN | libc::ENETUNREACH | libc::EHOSTUNREACH | libc::EHOSTDOWN | libc::ECONNREFUSED | libc::EADDRNOTAVAIL))
}

/// Принимает всё из сокета и раскладывает служебные сообщения; пакеты данных и прочее отдаёт в `f`.
fn drain_rx(ch: &mut Channel, rb: &mut RecvBatch, inbox: &mut Vec<Ctrl>, mut f: impl FnMut(Header, &[u8], &mut Channel)) {
    for _ in 0..8 {
        let n = match ch.sock.recv_batch(rb) {
            Ok(n) => n,
            Err(_) => return,
        };
        for i in 0..n {
            let p = rb.pkt_mut(i);
            let Some((h, len)) = ch.open(p) else { continue };
            ch.last_rx = Instant::now();
            let body = &rb.pkt(i)[HDR..HDR + len];
            match h.ty {
                T_CTRL | T_CTRLACK => {
                    let body = body.to_vec();
                    let _ = ch.on_ctrl_packet(h.ty, &body, inbox);
                }
                _ => f(h, body, ch),
            }
        }
        if n < rb.count {
            return;
        }
    }
}

fn flush_ctrl(ch: &mut Channel, rb: &mut RecvBatch) -> io::Result<()> {
    let t0 = Instant::now();
    let mut inbox = vec![];
    while ch.ctrl_pending() && t0.elapsed() < Duration::from_secs(2) {
        ch.pump_ctrl()?;
        drain_rx(ch, rb, &mut inbox, |_, _, _| {});
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

impl Scan {
    fn clone_scan(&self) -> Scan {
        Scan { manifest: self.manifest.clone(), sources: self.sources.clone(), items: self.items.clone() }
    }
}

fn send_data(ch: &mut Channel, s: Scan, done_list: Vec<(u32, String)>, sh: &Arc<Shared>, mut rb: RecvBatch) -> Outcome {
    let manifest = Arc::new(s.manifest);
    let nblocks = manifest.blocks();
    let (work_tx, work_rx) = mpsc::channel::<u32>();
    let (load_tx, load_rx) = mpsc::sync_channel(READAHEAD_BLOCKS);
    {
        let (m, src) = (manifest.clone(), Arc::new(s.sources));
        std::thread::Builder::new().name("dd-reader".into()).spawn(move || reader_thread(m, src, work_rx, load_tx)).expect("поток");
    }
    let claimed: HashMap<u32, [u8; 32]> = done_list
        .iter()
        .filter_map(|(b, h)| {
            let v = crate::crypto::unhex(h)?;
            Some((*b, v.try_into().ok()?))
        })
        .collect();
    // читаем всё подряд: блоки, которые уже есть у получателя, тоже считаем, чтобы сверить их хеши
    for b in 0..nblocks {
        work_tx.send(b).expect("поток чтения");
    }
    let mut to_load = nblocks as u64;

    let mut hashes: Vec<Option<[u8; 32]>> = vec![None; nblocks as usize];
    let mut active: HashMap<u32, Loaded> = HashMap::new();
    let mut first_queue: VecDeque<u32> = VecDeque::new();
    let mut cur: Option<(u32, usize)> = None;
    let mut retx: VecDeque<u32> = VecDeque::new();
    // Когда кусок ставили на повтор: пока прошлый ответ может быть в пути, новый запрос лишний.
    let mut retx_at: HashMap<u32, Instant> = HashMap::new();
    let mut reload: HashSet<u32> = HashSet::new();
    let mut repair_wait: HashSet<u32> = HashSet::new();
    let mut confirmed: Vec<bool> = vec![false; nblocks as usize];
    let mut confirm_out: VecDeque<u32> = VecDeque::new();
    let mut mismatches: HashMap<u32, u32> = HashMap::new();
    let mut verify_pending: Option<String> = None;
    let mut started = 0u32;
    let mut credit = RECV_WINDOW_BLOCKS;
    let mut next_first = 0u32;
    let mut cc = Cc::new();
    let mut last_budget = Instant::now();
    let mut budget = 0f64;
    let mut last_beat = Instant::now();
    let mut inbox = vec![];
    let mut highest_seen_seq = 0u64;
    let mut seq_moved_at = Instant::now();
    let mut resumed = 0u64;
    let mut sent_ok = 0u64;
    let mut progress = 0u64;
    let mut last_progress = Instant::now();
    let mut paused_local = false;
    let mut peer_paused = false;
    let mut out = vec![0u8; BATCH * MAX_PKT];
    let mut out_lens = [0usize; BATCH];
    let mut out_n = 0usize;
    let mut out_sent = 0usize;
    let mut beat_buf = vec![];
    #[cfg(target_os = "linux")]
    let mut use_gso = true;
    let beat_ms = env_u64("DECKDROP_BEAT_MS", 50);
    let mut was_paused = false;
    let moved_base = sh.moved.load(Relaxed);
    sh.set_phase("transfer");

    loop {
        // обратная связь
        let mut fbs: Vec<Feedback> = vec![];
        let mut echo = None;
        drain_rx(ch, &mut rb, &mut inbox, |h, body, c| {
            if h.ty == T_FEEDBACK {
                if let Some(f) = Feedback::decode(body) {
                    let now = c.now_us();
                    let rtt = now.wrapping_sub(f.echo_ts).wrapping_sub(f.echo_age_us) as f64;
                    if f.echo_ts != 0 && rtt < 5e6 {
                        echo = Some(rtt);
                    }
                    fbs.push(f);
                }
            }
        });
        if let Some(r) = echo {
            cc.on_rtt(r);
        }
        let now = Instant::now();
        for f in fbs {
            if !was_paused {
                cc.on_feedback(now, f.bytes, f.pkts, f.highest_seq);
            }
            if f.highest_seq > highest_seen_seq {
                highest_seen_seq = f.highest_seq;
                seq_moved_at = now;
            }
            credit = credit.max(f.credit);
            let retx_gap = Duration::from_micros((cc.srtt_us * 1.25).max(30_000.0) as u64);
            if retx_at.len() > 200_000 {
                retx_at.retain(|_, t| now.duration_since(*t) < retx_gap);
            }
            for (start, len) in f.nacks {
                for c in start..start.saturating_add(len as u32) {
                    let b = c / BLOCK_CHUNKS as u32;
                    if active.contains_key(&b) && !confirmed[b as usize] && !retx_at.get(&c).is_some_and(|t| now.duration_since(*t) < retx_gap) {
                        retx_at.insert(c, now);
                        retx.push_back(c);
                    }
                }
            }
            for (b, hash) in f.done {
                if b >= nblocks || confirmed[b as usize] {
                    if b < nblocks && confirmed[b as usize] {
                        confirm_out.push_back(b);
                    }
                    continue;
                }
                let Some(l) = active.get(&b) else { continue };
                if l.hash == hash {
                    confirmed[b as usize] = true;
                    sent_ok += manifest.block_len(b) as u64;
                    active.remove(&b);
                    confirm_out.push_back(b);
                    sh.blocks.lock().unwrap()[b as usize] = true;
                } else {
                    let m = mismatches.entry(b).or_insert(0);
                    *m += 1;
                    if *m > 5 {
                        ch.push_ctrl(&Ctrl::Error { message: format!("блок {b} не совпадает после {m} попыток") });
                        let _ = flush_ctrl(ch, &mut rb);
                        return Outcome::Failed(format!("блок {b} не совпадает; файл меняется во время передачи?"));
                    }
                    sh.repaired.fetch_add(1, Relaxed);
                    active.remove(&b);
                    reload.insert(b);
                    repair_wait.insert(b);
                    let _ = work_tx.send(b);
                    to_load += 1;
                }
            }
        }

        // служебные сообщения
        for m in inbox.drain(..) {
            match m {
                Ctrl::Pause => peer_paused = true,
                Ctrl::Resume => peer_paused = false,
                Ctrl::Cancel { reason } => return Outcome::Cancelled(reason),
                Ctrl::Error { message } => return Outcome::Failed(message),
                Ctrl::Repair { blocks } => {
                    for b in blocks {
                        if b >= nblocks {
                            continue;
                        }
                        if confirmed[b as usize] {
                            sent_ok = sent_ok.saturating_sub(manifest.block_len(b) as u64);
                        }
                        confirmed[b as usize] = false;
                        sh.blocks.lock().unwrap()[b as usize] = false;
                        sh.repaired.fetch_add(1, Relaxed);
                        reload.insert(b);
                        repair_wait.insert(b);
                        let _ = work_tx.send(b);
                        to_load += 1;
                    }
                }
                Ctrl::Phase { phase, done, total } => {
                    sh.set_phase(&phase);
                    sh.phase_done.store(done, Relaxed);
                    sh.phase_total.store(total, Relaxed);
                }
                Ctrl::Verify { root, .. } => verify_pending = Some(root),
                Ctrl::Finished => {
                    let _ = flush_ctrl(ch, &mut rb);
                    return Outcome::Completed;
                }
                _ => {}
            }
        }

        // сверка итогового хеша: ждём, пока прочитаны все блоки
        if to_load == 0 && verify_pending.is_some() && hashes.iter().all(Option::is_some) {
            let theirs = verify_pending.take().unwrap();
            let all: Vec<[u8; 32]> = hashes.iter().map(|h| h.unwrap()).collect();
            let ours = hex(&root_hash(&all));
            *sh.root.lock().unwrap() = ours.clone();
            let ok = ours == theirs;
            ch.push_ctrl(&Ctrl::VerifyResult { ok, root: ours });
            if !ok {
                let _ = flush_ctrl(ch, &mut rb);
                return Outcome::Failed("итоговый хеш не совпал".into());
            }
        }

        // загрузка блоков
        loop {
            if active.len() + first_queue.len() > RECV_WINDOW_BLOCKS as usize * 2 {
                break;
            }
            match load_rx.try_recv() {
                Ok(Ok((b, l))) => {
                    to_load -= 1;
                    hashes[b as usize] = Some(l.hash);
                    let was_repair = repair_wait.remove(&b);
                    reload.remove(&b);
                    if was_repair {
                        // получатель запросит куски сам после Reject
                        ch.push_ctrl(&Ctrl::Reject { block: b });
                        active.insert(b, l);
                    } else if let Some(h) = claimed.get(&b) {
                        if *h == l.hash {
                            confirmed[b as usize] = true;
                            sh.blocks.lock().unwrap()[b as usize] = true;
                            resumed += manifest.block_len(b) as u64;
                        } else {
                            // Блок другой: сообщаем об этом и шлём его обычным порядком.
                            ch.push_ctrl(&Ctrl::Reject { block: b });
                            active.insert(b, l);
                            first_queue.push_back(b);
                        }
                    } else {
                        active.insert(b, l);
                        first_queue.push_back(b);
                    }
                }
                Ok(Err(e)) => {
                    ch.push_ctrl(&Ctrl::Error { message: e.clone() });
                    let _ = flush_ctrl(ch, &mut rb);
                    return Outcome::Failed(e);
                }
                Err(_) => break,
            }
        }

        if let Err(e) = ch.pump_ctrl() {
            if is_net_down(&e) {
                return Outcome::LinkLost;
            }
        }

        // отмена, пауза, тишина
        if sh.cancel.load(Relaxed) {
            ch.push_ctrl(&Ctrl::Cancel { reason: "отменено отправителем".into() });
            let _ = flush_ctrl(ch, &mut rb);
            return Outcome::Cancelled("отменено".into());
        }
        let want_pause = sh.pause.load(Relaxed);
        if want_pause != paused_local {
            paused_local = want_pause;
            ch.push_ctrl(if want_pause { &Ctrl::Pause } else { &Ctrl::Resume });
        }
        if ch.last_rx.elapsed() > LINK_TIMEOUT {
            return Outcome::LinkLost;
        }
        let paused = paused_local || peer_paused;
        // Нечего слать (например, перечитываем уже принятые блоки): канал здесь ни при чём.
        let idle = paused || (cur.is_none() && retx.is_empty() && first_queue.is_empty());
        if idle != was_paused {
            if idle {
                cc.on_pause();
            } else {
                cc.on_resume();
            }
            was_paused = idle;
        }

        // сигнал получателю
        let beat_gap = if paused { 200 } else { beat_ms };
        if last_beat.elapsed() >= Duration::from_millis(beat_gap) {
            last_beat = Instant::now();
            let n = confirm_out.len().min(Beat::MAX_CONFIRMED);
            let b = Beat { next_first_chunk: next_first, paused, confirmed: confirm_out.drain(..n).collect() };
            b.encode(&mut beat_buf);
            let body = std::mem::take(&mut beat_buf);
            let r = ch.send_plain(T_BEAT, &body);
            beat_buf = body;
            if let Err(e) = r {
                if is_net_down(&e) {
                    return Outcome::LinkLost;
                }
            }
        }

        // показатели
        // Считаем подтверждённые блоки: при потерях число отправленных байт ничего не говорит о ходе передачи.
        let good = (resumed + sent_ok).min(sh.total.load(Relaxed));
        progress = progress.max(good);
        sh.bytes.fetch_max(progress, Relaxed);
        if last_progress.elapsed() > Duration::from_secs(1) {
            last_progress = Instant::now();
            ch.push_ctrl(&Ctrl::Phase { phase: "progress".into(), done: progress, total: 0 });
        }
        sh.moved.store(moved_base + sent_ok, Relaxed);
        sh.rate.store(cc.rate as u64, Relaxed);
        sh.rtt_us.store(cc.srtt_us as u64, Relaxed);

        // темп отправки
        let now = Instant::now();
        budget += cc.rate * now.duration_since(last_budget).as_secs_f64();
        last_budget = now;
        let cap = (cc.rate * 0.002).max((BATCH * CHUNK) as f64);
        budget = budget.min(cap);

        if paused {
            std::thread::sleep(Duration::from_millis(5));
            budget = 0.0;
            continue;
        }

        // собираем пачку
        if out_sent >= out_n {
            out_n = 0;
            out_sent = 0;
            // Если всё, что в пути, потерялось, подтверждать некому: без этого передача встанет навсегда.
            if ch.seq.saturating_sub(highest_seen_seq) >= cc.window_pkts()
                && now.duration_since(seq_moved_at) > Duration::from_micros((cc.srtt_us * 2.0).max(300_000.0) as u64)
            {
                highest_seen_seq = ch.seq;
                seq_moved_at = now;
                cc.on_stall();
                sh.stall_lost.fetch_add(1, Relaxed);
            }
            let in_path = ch.seq.saturating_sub(highest_seen_seq);
            if in_path >= cc.window_pkts() {
                sh.stall_window.fetch_add(1, Relaxed);
            } else if budget < CHUNK as f64 {
                sh.stall_budget.fetch_add(1, Relaxed);
            } else if retx.is_empty() && cur.is_none() {
                if first_queue.is_empty() {
                    sh.stall_nodata.fetch_add(1, Relaxed);
                } else if started >= credit {
                    sh.stall_credit.fetch_add(1, Relaxed);
                }
            }
            if in_path < cc.window_pkts() {
                while out_n < BATCH && budget >= CHUNK as f64 {
                    let (chunk, retrans) = if let Some(c) = retx.pop_front() {
                        (c, true)
                    } else {
                        if cur.is_none() && started < credit {
                            if let Some(b) = first_queue.pop_front() {
                                cur = Some((b, 0));
                                started += 1;
                            }
                        }
                        let Some((b, i)) = cur else { break };
                        let n = manifest.block_len(b).div_ceil(CHUNK);
                        cur = if i + 1 >= n { None } else { Some((b, i + 1)) };
                        (b * BLOCK_CHUNKS as u32 + i as u32, false)
                    };
                    let b = chunk / BLOCK_CHUNKS as u32;
                    let i = (chunk % BLOCK_CHUNKS as u32) as usize;
                    let Some(l) = active.get(&b) else { continue };
                    let s = i * CHUNK;
                    if s >= l.data.len() {
                        continue;
                    }
                    let e = (s + CHUNK).min(l.data.len());
                    let flags = if retrans { F_RETX } else { 0 };
                    let (data, slot) = (&l.data[s..e], &mut out[out_n * MAX_PKT..(out_n + 1) * MAX_PKT]);
                    out_lens[out_n] = ch.seal_into(T_DATA, flags, chunk, data, slot);
                    out_n += 1;
                    // Оценка канала считает только полезные байты, поэтому и списываем их же.
                    budget -= (e - s) as f64;
                    sh.sent_pkts.fetch_add(1, Relaxed);
                    if retrans {
                        retx_at.insert(chunk, now);
                        sh.retx.fetch_add(1, Relaxed);
                    } else {
                        next_first = chunk + 1;
                    }
                }
            }
        }

        if out_sent < out_n {
            let cnt = out_n - out_sent;
            // На Linux пачка одинаковых пакетов уходит одним вызовом: замер дал вдвое меньше нагрузки на ЦП.
            #[cfg(target_os = "linux")]
            if use_gso && cnt > 1 && out_lens[out_sent..out_n].iter().all(|l| *l == MAX_PKT) {
                match ch.sock.send_gso(&out[out_sent * MAX_PKT..out_n * MAX_PKT], MAX_PKT as u16) {
                    Ok(_) => {
                        out_sent = out_n;
                        continue;
                    }
                    Err(e) if ddnet::is_transient_send_error(&e) => {
                        sh.stall_enobufs.fetch_add(1, Relaxed);
                        std::thread::sleep(Duration::from_micros(200));
                        continue;
                    }
                    Err(e) if is_net_down(&e) => return Outcome::LinkLost,
                    Err(_) => use_gso = false,
                }
            }
            let mut refs: [&[u8]; BATCH] = [&[]; BATCH];
            for k in 0..cnt {
                let j = out_sent + k;
                refs[k] = &out[j * MAX_PKT..j * MAX_PKT + out_lens[j]];
            }
            match ch.sock.send_batch(&refs[..cnt]) {
                Ok(n) => out_sent += n,
                Err(e) if ddnet::is_transient_send_error(&e) => {
                    sh.stall_enobufs.fetch_add(1, Relaxed);
                    std::thread::sleep(Duration::from_micros(200));
                }
                Err(e) if is_net_down(&e) => return Outcome::LinkLost,
                Err(e) => return io_fail(e),
            }
        } else {
            std::thread::sleep(Duration::from_micros(100));
        }
    }
}

// ========== получатель ==========

pub struct OfferInfo {
    pub job: String,
    pub name: String,
    pub items: Vec<Item>,
    pub total: u64,
    pub dest: Option<String>,
    pub overwrite: bool,
    pub from_llm: bool,
    pub in_place: bool,
}

pub enum Decision {
    Accept { dest: PathBuf, exclude: Vec<String> },
    Decline(String),
}

pub struct Slot {
    buf: Vec<u8>,
    got: [u64; BLOCK_CHUNKS / 64],
    count: usize,
    need: usize,
}

impl Slot {
    fn has(&self, i: usize) -> bool {
        self.got[i / 64] >> (i % 64) & 1 == 1
    }
}

fn part_dir(dest: &Path, id: &str) -> PathBuf {
    dest.join(format!(".deckdrop-{id}"))
}

/// Создаёт файл в рабочей папке, выставляя размер.
fn open_part(root: &Path, e: &Entry) -> io::Result<File> {
    let p = root.join(safe_rel(&e.path).ok_or_else(|| io::Error::other("небезопасный путь"))?);
    if let Some(d) = p.parent() {
        fs::create_dir_all(d)?;
    }
    let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&p)?;
    if f.metadata()?.len() != e.size {
        f.set_len(e.size)?;
    }
    Ok(f)
}

enum WriteDone {
    Ok { block: u32, hash: [u8; 32], buf: Vec<u8> },
    Err(String),
}

fn writer_thread(manifest: Arc<Manifest>, root: PathBuf, jobs: Arc<Mutex<Receiver<(u32, Vec<u8>, usize)>>>, done: mpsc::Sender<WriteDone>) {
    let files = manifest.files();
    let mut cache = FileCache::new(64);
    loop {
        let job = jobs.lock().unwrap().recv();
        let Ok((b, buf, len)) = job else { return };
        let data = &buf[..len];
        let hash = *blake3::hash(data).as_bytes();
        let r = for_ranges(&files, b as u64 * BLOCK as u64, len, |e, off, pos, n| {
            cache.get(&e.path, || open_part(&root, e)).and_then(|f| f.write_all_at(&data[pos..pos + n], off)).map_err(|err| format!("{}: {err}", e.path))
        });
        let msg = match r {
            Ok(()) => WriteDone::Ok { block: b, hash, buf },
            Err(e) => WriteDone::Err(e),
        };
        if done.send(msg).is_err() {
            return;
        }
    }
}

fn load_journal(path: &Path, nblocks: u32) -> Vec<Option<[u8; 32]>> {
    let mut out = vec![None; nblocks as usize];
    if let Ok(d) = fs::read(path) {
        for r in d.as_chunks::<36>().0 {
            let b = u32::from_le_bytes(r[..4].try_into().unwrap());
            if b < nblocks {
                out[b as usize] = Some(r[4..].try_into().unwrap());
            }
        }
    }
    out
}

/// Освобождает кеш страниц и читает с диска, чтобы сверка проверяла именно записанное.
fn verify_thread(manifest: Arc<Manifest>, root: PathBuf, want: Vec<(u32, [u8; 32])>, progress: mpsc::Sender<Result<u32, u32>>) {
    let files = manifest.files();
    let mut cache: FileCache = FileCache::new(32);
    for e in &files {
        if e.size == 0 {
            continue;
        }
        if let Ok(f) = open_part(&root, e) {
            let _ = f.sync_all();
            drop_cache(&f);
        }
    }
    let mut buf = vec![0u8; BLOCK];
    for (b, h) in want {
        let len = manifest.block_len(b);
        buf[..len].fill(0);
        let r: Result<(), io::Error> = for_ranges(&files, b as u64 * BLOCK as u64, len, |e, off, pos, n| {
            let f = cache.get(&e.path, || {
                let f = File::open(root.join(safe_rel(&e.path).unwrap()))?;
                no_cache(&f);
                Ok(f)
            })?;
            f.read_exact_at(&mut buf[pos..pos + n], off)
        });
        let ok = r.is_ok() && *blake3::hash(&buf[..len]).as_bytes() == h;
        if progress.send(if ok { Ok(b) } else { Err(b) }).is_err() {
            return;
        }
    }
}

#[cfg(target_os = "linux")]
fn drop_cache(f: &File) {
    use std::os::fd::AsRawFd;
    unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
}
#[cfg(not(target_os = "linux"))]
fn drop_cache(_: &File) {}

#[cfg(target_os = "macos")]
fn no_cache(f: &File) {
    use std::os::fd::AsRawFd;
    unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) };
}
#[cfg(not(target_os = "macos"))]
fn no_cache(_: &File) {}

fn set_mtime(path: &Path, s: i64, ns: u32, follow: bool) {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_bytes()) else { return };
    let ts = [libc::timespec { tv_sec: s, tv_nsec: ns as _ }, libc::timespec { tv_sec: s, tv_nsec: ns as _ }];
    let flag = if follow { 0 } else { libc::AT_SYMLINK_NOFOLLOW };
    unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), ts.as_ptr(), flag) };
}

/// Переносит принятое в папку назначения, ставит права и время изменения.
fn finalize(m: &Manifest, root: &Path, dest: &Path, overwrite: bool, in_place: bool) -> io::Result<()> {
    for e in &m.entries {
        let rel = safe_rel(&e.path).ok_or_else(|| io::Error::other("небезопасный путь"))?;
        let target = dest.join(&rel);
        match e.kind {
            'd' => fs::create_dir_all(&target)?,
            'f' if in_place => {
                // Файл уже на месте. Пустой мог ещё не появиться.
                if !target.exists() {
                    if let Some(d) = target.parent() {
                        fs::create_dir_all(d)?;
                    }
                    File::create(&target)?;
                }
                fs::set_permissions(&target, fs::Permissions::from_mode(e.mode & 0o777))?;
                set_mtime(&target, e.mtime_s, e.mtime_ns, true);
            }
            'f' => {
                if let Some(d) = target.parent() {
                    fs::create_dir_all(d)?;
                }
                if !overwrite && target.exists() {
                    return Err(io::Error::other(format!("файл уже существует: {}", e.path)));
                }
                let src = root.join(&rel);
                if e.size == 0 && !src.exists() {
                    File::create(&src).or_else(|_| {
                        fs::create_dir_all(src.parent().unwrap())?;
                        File::create(&src)
                    })?;
                }
                fs::set_permissions(&src, fs::Permissions::from_mode(e.mode & 0o777))?;
                set_mtime(&src, e.mtime_s, e.mtime_ns, true);
                fs::rename(&src, &target)?;
            }
            'l' => {
                let t = e.target.as_deref().unwrap_or("");
                if safe_symlink(&e.path, t) {
                    if let Some(d) = target.parent() {
                        fs::create_dir_all(d)?;
                    }
                    if overwrite {
                        let _ = fs::remove_file(&target);
                    }
                    if fs::symlink_metadata(&target).is_err() {
                        std::os::unix::fs::symlink(t, &target)?;
                        set_mtime(&target, e.mtime_s, e.mtime_ns, false);
                    }
                }
            }
            _ => {}
        }
    }
    // время изменения папок: после всех файлов, от глубоких к верхним
    for e in m.entries.iter().rev().filter(|e| e.kind == 'd') {
        let t = dest.join(safe_rel(&e.path).unwrap());
        let _ = fs::set_permissions(&t, fs::Permissions::from_mode((e.mode & 0o777) | 0o700));
        set_mtime(&t, e.mtime_s, e.mtime_ns, true);
    }
    Ok(())
}

pub fn free_space(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let mut p = path.to_path_buf();
    while !p.exists() {
        p = p.parent()?.to_path_buf();
    }
    let c = std::ffi::CString::new(p.as_os_str().as_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    Some(s.f_bavail as u64 * s.f_frsize as u64)
}

pub fn run_receiver(ch: &mut Channel, sh: &Arc<Shared>, mut on_offer: impl FnMut(&OfferInfo) -> Option<Decision>) -> Outcome {
    let mut rb = RecvBatch::new(BATCH.max(64), 2048);
    let _ = ch.sock.set_recv_timeout(Some(Duration::from_millis(2)));
    sh.set_phase("waiting_offer");
    let mut inbox = vec![];
    let mut offer: Option<OfferInfo> = None;
    let mut decided: Option<(PathBuf, bool, bool)> = None;
    let mut last_beat = Instant::now();
    let mut chans: Vec<Ctrl> = vec![];
    // согласование
    loop {
        if ch.pump_ctrl().is_err() {
            return Outcome::LinkLost;
        }
        drain_rx(ch, &mut rb, &mut inbox, |_, _, _| {});
        chans.append(&mut inbox);
        for m in chans.drain(..) {
            match m {
                Ctrl::Offer { job, name, items, total, dest, overwrite, from_llm, in_place } => {
                    sh.total.store(total, Relaxed);
                    offer = Some(OfferInfo { job, name, items, total, dest, overwrite, from_llm, in_place });
                    sh.set_phase("offer");
                }
                Ctrl::Cancel { reason } => return Outcome::Cancelled(reason),
                Ctrl::Error { message } => return Outcome::Failed(message),
                Ctrl::Manifest { manifest } => {
                    if let Err(e) = validate(&manifest) {
                        ch.push_ctrl(&Ctrl::Decline { reason: e.clone() });
                        let _ = flush_ctrl(ch, &mut rb);
                        return Outcome::Failed(e);
                    }
                    let Some((dest, overwrite, in_place)) = decided.clone() else {
                        return Outcome::Failed("манифест раньше согласия".into());
                    };
                    return recv_data(ch, Arc::new(manifest), dest, overwrite, in_place, sh, rb);
                }
                _ => {}
            }
        }
        if decided.is_none() {
            if let Some(o) = &offer {
                match on_offer(o) {
                    Some(Decision::Accept { dest, exclude }) => {
                        decided = Some((dest.clone(), o.overwrite, o.in_place));
                        ch.push_ctrl(&Ctrl::Accept { exclude, dest: dest.to_string_lossy().into_owned() });
                        sh.set_phase("preparing");
                    }
                    Some(Decision::Decline(reason)) => {
                        ch.push_ctrl(&Ctrl::Decline { reason: reason.clone() });
                        let _ = flush_ctrl(ch, &mut rb);
                        return Outcome::Declined(reason);
                    }
                    None => {}
                }
            }
        }
        if sh.cancel.load(Relaxed) {
            ch.push_ctrl(&Ctrl::Cancel { reason: "отменено получателем".into() });
            let _ = flush_ctrl(ch, &mut rb);
            return Outcome::Cancelled("отменено".into());
        }
        if ch.last_rx.elapsed() > LINK_TIMEOUT {
            return Outcome::LinkLost;
        }
        if last_beat.elapsed() > Duration::from_millis(200) {
            last_beat = Instant::now();
            let _ = ch.send_plain(T_BEAT, &[0, 0, 0, 0, 0, 0]);
        }
    }
}

fn recv_data(ch: &mut Channel, m: Arc<Manifest>, dest: PathBuf, overwrite: bool, in_place: bool, sh: &Arc<Shared>, mut rb: RecvBatch) -> Outcome {
    let id = m.id();
    // На месте: данные сразу пишутся в существующие файлы, рядом хранится только журнал.
    let part = if in_place { crate::node::data_dir().join("inplace").join(&id) } else { part_dir(&dest, &id) };
    let root = if in_place { dest.clone() } else { part.join("data") };
    let overwrite = overwrite || in_place;
    sh.total.store(m.total, Relaxed);
    *sh.manifest.lock().unwrap() = Some(m.clone());
    let nblocks = m.blocks();

    if let Err(e) = fs::create_dir_all(&part) {
        return Outcome::Failed(e.to_string());
    }
    if in_place && !part.join("blocks.bin").exists() {
        if let Err(o) = seed_journal(ch, &m, &root, &part.join("blocks.bin"), sh, &mut rb) {
            return o;
        }
    }
    if let Err(e) = prepare_part(&m, &root, in_place) {
        let msg = format!("не удалось подготовить файлы: {e}");
        ch.push_ctrl(&Ctrl::Error { message: msg.clone() });
        let _ = flush_ctrl(ch, &mut rb);
        return Outcome::Failed(msg);
    }
    let journal_path = part.join("blocks.bin");
    let mut written: Vec<Option<[u8; 32]>> = load_journal(&journal_path, nblocks);
    let mut journal = match OpenOptions::new().create(true).append(true).mode(0o600).open(&journal_path) {
        Ok(j) => j,
        Err(e) => return Outcome::Failed(e.to_string()),
    };
    let done_list: Vec<(u32, String)> = written.iter().enumerate().filter_map(|(b, h)| h.map(|h| (b as u32, hex(&h)))).collect();
    let mut bytes: u64 = done_list.iter().map(|(b, _)| m.block_len(*b) as u64).sum();
    sh.bytes.store(bytes, Relaxed);
    {
        let mut bl = sh.blocks.lock().unwrap();
        *bl = written.iter().map(Option::is_some).collect();
    }
    ch.push_ctrl(&Ctrl::Ready { done: done_list });

    // 0 нет, 1 приём, 2 запись, 3 записан
    let mut state: Vec<u8> = written.iter().map(|h| if h.is_some() { 3 } else { 0 }).collect();
    let mut slots: HashMap<u32, Slot> = HashMap::new();
    let mut pool: Vec<Vec<u8>> = vec![];
    let (job_tx, job_rx) = mpsc::channel();
    let job_rx = Arc::new(Mutex::new(job_rx));
    let (done_tx, done_rx) = mpsc::channel();
    let writers = 2;
    for i in 0..writers {
        let (mm, r, j, d) = (m.clone(), root.clone(), job_rx.clone(), done_tx.clone());
        std::thread::Builder::new().name(format!("dd-writer-{i}")).spawn(move || writer_thread(mm, r, j, d)).expect("поток");
    }
    let mut released = 0u32;
    let mut unconfirmed: VecDeque<(u32, [u8; 32])> = VecDeque::new();
    let mut unconfirmed_set: HashSet<u32> = HashSet::new();
    let mut missing: VecDeque<(u32, Instant)> = VecDeque::new();
    let mut next_first = 0u32;
    let (mut rx_bytes, mut rx_pkts, mut highest_seq) = (0u64, 0u64, 0u64);
    let (mut echo_ts, mut echo_at) = (0u32, Instant::now());
    let mut last_fb = Instant::now();
    let mut fb_buf = vec![];
    let mut inbox = vec![];
    let mut paused_local = false;
    let mut verify: Option<Receiver<Result<u32, u32>>> = None;
    let mut bad: Vec<u32> = vec![];
    let mut verified = 0u64;
    let mut last_phase = Instant::now();
    // На месте получатель не знает, какие блоки совпадут: ход передачи сообщает отправитель.
    let mut sender_progress = 0u64;
    let mut stage = 0u8; // 0 приём, 1 сверка с диска, 2 ждём ответ по хешу
    let mut repairs_total = 0u32;
    let mut verify_rounds = 0u32;
    let fb_ms = env_u64("DECKDROP_FB_MS", 20);
    let sim_loss = sim_env("DECKDROP_SIM_LOSS");
    let sim_rate = sim_env("DECKDROP_SIM_RATE") * 1e6;
    let mut sim_tokens = 0f64;
    let mut sim_t = Instant::now();
    let mut sim_rng = 0x9E3779B97F4A7C15u64;
    sh.set_phase("transfer");

    let mark_missing = |c: u32, state: &[u8], slots: &HashMap<u32, Slot>, missing: &mut VecDeque<(u32, Instant)>, now: Instant| {
        let b = c / BLOCK_CHUNKS as u32;
        let i = (c % BLOCK_CHUNKS as u32) as usize;
        if b >= nblocks || state[b as usize] >= 2 || slots.get(&b).is_some_and(|s| s.has(i)) {
            return;
        }
        missing.push_back((c, now));
    };

    loop {
        let now = Instant::now();
        let n = match ch.sock.recv_batch(&mut rb) {
            Ok(n) => n,
            Err(e) if ddnet::is_timeout(&e) => 0,
            Err(e) if matches!(e.raw_os_error(), Some(libc::ECONNREFUSED)) => 0,
            Err(e) => return io_fail(e),
        };
        for k in 0..n {
            let p = rb.pkt_mut(k);
            let Some((h, len)) = ch.open(p) else { continue };
            ch.last_rx = now;
            match h.ty {
                T_DATA => {
                    if sim_loss > 0.0 || sim_rate > 0.0 {
                        sim_rng ^= sim_rng << 13;
                        sim_rng ^= sim_rng >> 7;
                        sim_rng ^= sim_rng << 17;
                        if (sim_rng as f64 / u64::MAX as f64) < sim_loss {
                            continue;
                        }
                        if sim_rate > 0.0 {
                            let t = Instant::now();
                            sim_tokens = (sim_tokens + sim_rate * t.duration_since(sim_t).as_secs_f64()).min(2e6);
                            sim_t = t;
                            if sim_tokens < len as f64 {
                                continue;
                            }
                            sim_tokens -= len as f64;
                        }
                    }
                    rx_bytes += len as u64;
                    rx_pkts += 1;
                    highest_seq = highest_seq.max(h.seq);
                    echo_ts = h.ts;
                    echo_at = now;
                    let c = h.chunk;
                    let b = c / BLOCK_CHUNKS as u32;
                    let i = (c % BLOCK_CHUNKS as u32) as usize;
                    if b >= nblocks {
                        continue;
                    }
                    if h.flags & F_RETX == 0 && c >= next_first {
                        let mut g = next_first;
                        while g < c {
                            let gb = g / BLOCK_CHUNKS as u32;
                            if gb < nblocks && state[gb as usize] >= 2 {
                                g = (gb + 1) * BLOCK_CHUNKS as u32;
                                continue;
                            }
                            mark_missing(g, &state, &slots, &mut missing, now);
                            g += 1;
                        }
                        next_first = c + 1;
                    }
                    if state[b as usize] >= 2 {
                        continue;
                    }
                    if !slots.contains_key(&b) {
                        if slots.len() >= RECV_WINDOW_BLOCKS as usize + 8 {
                            continue; // запросим позже
                        }
                        let bl = m.block_len(b);
                        let mut buf = pool.pop().unwrap_or_default();
                        buf.resize(BLOCK, 0);
                        slots.insert(b, Slot { buf, got: [0; BLOCK_CHUNKS / 64], count: 0, need: bl.div_ceil(CHUNK) });
                        state[b as usize] = 1;
                    }
                    let s = slots.get_mut(&b).unwrap();
                    if !s.has(i) && i < s.need {
                        let off = i * CHUNK;
                        s.buf[off..off + len].copy_from_slice(&rb.buf[k * rb.pkt_size + HDR..k * rb.pkt_size + HDR + len]);
                        s.got[i / 64] |= 1 << (i % 64);
                        s.count += 1;
                        bytes += len as u64;
                        sh.moved.fetch_add(len as u64, Relaxed);
                        if s.count == s.need {
                            let s = slots.remove(&b).unwrap();
                            state[b as usize] = 2;
                            let _ = job_tx.send((b, s.buf, m.block_len(b)));
                        }
                    }
                }
                T_BEAT => {
                    let body = rb.buf[k * rb.pkt_size + HDR..k * rb.pkt_size + HDR + len].to_vec();
                    if let Some(beat) = Beat::decode(&body) {
                        if beat.next_first_chunk > next_first {
                            let mut g = next_first;
                            while g < beat.next_first_chunk {
                                let gb = g / BLOCK_CHUNKS as u32;
                                if gb < nblocks && state[gb as usize] >= 2 {
                                    g = (gb + 1) * BLOCK_CHUNKS as u32;
                                    continue;
                                }
                                mark_missing(g, &state, &slots, &mut missing, now);
                                g += 1;
                            }
                            next_first = beat.next_first_chunk;
                        }
                        for c in beat.confirmed {
                            if unconfirmed_set.remove(&c) {
                                unconfirmed.retain(|x| x.0 != c);
                            }
                        }
                    }
                }
                T_CTRL | T_CTRLACK => {
                    let body = rb.buf[k * rb.pkt_size + HDR..k * rb.pkt_size + HDR + len].to_vec();
                    let _ = ch.on_ctrl_packet(h.ty, &body, &mut inbox);
                }
                _ => {}
            }
        }
        sh.bytes.store(if in_place { sender_progress } else { bytes.min(m.total) }, Relaxed);

        // запись закончена
        loop {
            match done_rx.try_recv() {
                Ok(WriteDone::Ok { block, hash, buf }) => {
                    pool.push(buf);
                    state[block as usize] = 3;
                    written[block as usize] = Some(hash);
                    released += 1;
                    let mut rec = [0u8; 36];
                    rec[..4].copy_from_slice(&block.to_le_bytes());
                    rec[4..].copy_from_slice(&hash);
                    if let Err(e) = journal.write_all(&rec) {
                        return io_fail(e);
                    }
                    if unconfirmed_set.insert(block) {
                        unconfirmed.push_back((block, hash));
                    }
                    sh.blocks.lock().unwrap()[block as usize] = true;
                }
                Ok(WriteDone::Err(e)) => {
                    ch.push_ctrl(&Ctrl::Error { message: e.clone() });
                    let _ = flush_ctrl(ch, &mut rb);
                    return Outcome::Failed(e);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        // служебные
        for msg in std::mem::take(&mut inbox) {
            match msg {
                Ctrl::Pause => sh.set_phase("paused_by_peer"),
                Ctrl::Phase { phase, done, .. } if phase == "progress" => sender_progress = done,
                Ctrl::Resume => sh.set_phase("transfer"),
                Ctrl::Cancel { reason } => {
                    let _ = fs::remove_dir_all(&part);
                    return Outcome::Cancelled(reason);
                }
                Ctrl::Error { message } => return Outcome::Failed(message),
                Ctrl::Reject { block } if block < nblocks => {
                    // блок нужно принять заново: хеш не совпал у отправителя или при сверке
                    if !in_place {
                        repairs_total += 1;
                        sh.repaired.fetch_add(1, Relaxed);
                    }
                    written[block as usize] = None;
                    state[block as usize] = 0;
                    sh.blocks.lock().unwrap()[block as usize] = false;
                    let lenb = m.block_len(block);
                    bytes = bytes.saturating_sub(lenb as u64);
                    if unconfirmed_set.remove(&block) {
                        unconfirmed.retain(|x| x.0 != block);
                    }
                    let first = block * BLOCK_CHUNKS as u32;
                    // Куски дальше первой передачи ещё придут сами, разрывы в них найдутся как обычно.
                    for c in (first..first + lenb.div_ceil(CHUNK) as u32).filter(|c| *c < next_first) {
                        missing.push_back((c, now));
                    }
                    stage = 0;
                }
                Ctrl::VerifyResult { ok, root: their_root } => {
                    *sh.root.lock().unwrap() = their_root;
                    if !ok {
                        ch.push_ctrl(&Ctrl::Error { message: "итоговый хеш не совпал".into() });
                        let _ = flush_ctrl(ch, &mut rb);
                        return Outcome::Failed("итоговый хеш не совпал".into());
                    }
                    sh.set_phase("finalizing");
                    drop(job_tx);
                    if let Err(e) = finalize(&m, &root, &dest, overwrite, in_place) {
                        let msg = format!("не удалось перенести файлы: {e}");
                        ch.push_ctrl(&Ctrl::Error { message: msg.clone() });
                        let _ = flush_ctrl(ch, &mut rb);
                        return Outcome::Failed(msg);
                    }
                    let _ = fs::remove_dir_all(&part);
                    ch.push_ctrl(&Ctrl::Finished);
                    let _ = flush_ctrl(ch, &mut rb);
                    return Outcome::Completed;
                }
                _ => {}
            }
        }

        // отмена, пауза, тишина
        if sh.cancel.load(Relaxed) {
            ch.push_ctrl(&Ctrl::Cancel { reason: "отменено получателем".into() });
            let _ = flush_ctrl(ch, &mut rb);
            let _ = fs::remove_dir_all(&part);
            return Outcome::Cancelled("отменено".into());
        }
        let want_pause = sh.pause.load(Relaxed);
        if want_pause != paused_local {
            paused_local = want_pause;
            ch.push_ctrl(if want_pause { &Ctrl::Pause } else { &Ctrl::Resume });
        }
        if ch.last_rx.elapsed() > LINK_TIMEOUT {
            return Outcome::LinkLost;
        }
        if ch.pump_ctrl().is_err() {
            return Outcome::LinkLost;
        }

        // обратная связь
        if now.duration_since(last_fb) >= Duration::from_millis(fb_ms) {
            last_fb = now;
            let mut nacks: Vec<(u32, u16)> = vec![];
            let mut keep = VecDeque::with_capacity(missing.len());
            let due = now - Duration::from_millis(1);
            let renack = now + Duration::from_millis(25);
            while let Some((c, at)) = missing.pop_front() {
                let b = c / BLOCK_CHUNKS as u32;
                let i = (c % BLOCK_CHUNKS as u32) as usize;
                if state[b as usize] >= 2 || slots.get(&b).is_some_and(|s| s.has(i)) {
                    continue;
                }
                if at <= due && nacks.len() < Feedback::MAX_NACKS * 4 {
                    match nacks.last_mut() {
                        Some(l) if l.0 + l.1 as u32 == c && l.1 < u16::MAX => l.1 += 1,
                        _ => nacks.push((c, 1)),
                    }
                    keep.push_back((c, renack));
                } else {
                    keep.push_back((c, at));
                }
            }
            missing = keep;
            let credit = released + RECV_WINDOW_BLOCKS;
            let rotate = unconfirmed.len().min(Feedback::MAX_DONE);
            let done: Vec<(u32, [u8; 32])> = unconfirmed.iter().take(rotate).cloned().collect();
            unconfirmed.rotate_left(rotate);
            let mut parts = nacks.chunks(Feedback::MAX_NACKS);
            loop {
                let part = parts.next().unwrap_or(&[]);
                let f = Feedback {
                    bytes: rx_bytes,
                    pkts: rx_pkts,
                    highest_seq,
                    echo_ts,
                    echo_age_us: now.duration_since(echo_at).as_micros() as u32,
                    credit,
                    done: done.clone(),
                    nacks: part.to_vec(),
                };
                f.encode(&mut fb_buf);
                let body = std::mem::take(&mut fb_buf);
                let r = ch.send_plain(T_FEEDBACK, &body);
                fb_buf = body;
                if r.is_err() {
                    return Outcome::LinkLost;
                }
                if parts.len() == 0 {
                    break;
                }
            }
        }

        // все блоки записаны и подтверждены: сверка с диска
        let all_written = stage == 0 && state.iter().all(|s| *s == 3) && unconfirmed.is_empty();
        if all_written && verify.is_none() {
            stage = 1;
            verify_rounds += 1;
            verified = 0;
            bad.clear();
            sh.set_phase("verifying");
            ch.push_ctrl(&Ctrl::Phase { phase: "verifying".into(), done: 0, total: m.total });
            sh.phase_total.store(m.total, Relaxed);
            sh.phase_done.store(0, Relaxed);
            let want: Vec<(u32, [u8; 32])> = written.iter().enumerate().map(|(b, h)| (b as u32, h.unwrap())).collect();
            let (tx, rx) = mpsc::channel();
            let (mm, rr) = (m.clone(), root.clone());
            std::thread::Builder::new().name("dd-verify".into()).spawn(move || verify_thread(mm, rr, want, tx)).expect("поток");
            verify = Some(rx);
        }
        if let Some(v) = &verify {
            let mut finished = false;
            loop {
                match v.try_recv() {
                    Ok(Ok(b)) => verified += m.block_len(b) as u64,
                    Ok(Err(b)) => {
                        verified += m.block_len(b) as u64;
                        bad.push(b);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        finished = true;
                        break;
                    }
                }
            }
            sh.phase_done.store(verified, Relaxed);
            if last_phase.elapsed() > Duration::from_secs(1) {
                last_phase = Instant::now();
                ch.push_ctrl(&Ctrl::Phase { phase: "verifying".into(), done: verified, total: m.total });
            }
            if finished {
                verify = None;
                if written.iter().any(Option::is_none) {
                    // Пока шла сверка, отправитель забраковал блок: дождёмся его и сверим заново.
                    stage = 0;
                } else if bad.is_empty() {
                    stage = 2;
                    let all: Vec<[u8; 32]> = written.iter().map(|h| h.unwrap()).collect();
                    let r = hex(&root_hash(&all));
                    *sh.root.lock().unwrap() = r.clone();
                    ch.push_ctrl(&Ctrl::Verify { root: r, repaired: repairs_total });
                    sh.set_phase("confirming");
                } else if verify_rounds >= 4 {
                    let msg = format!("сверка с диска не прошла: {} блоков", bad.len());
                    ch.push_ctrl(&Ctrl::Error { message: msg.clone() });
                    let _ = flush_ctrl(ch, &mut rb);
                    return Outcome::Failed(msg);
                } else {
                    // Отправитель перечитает блоки и пришлёт Reject, после чего они будут приняты заново.
                    ch.push_ctrl(&Ctrl::Repair { blocks: bad.clone() });
                    stage = 0;
                    for b in &bad {
                        state[*b as usize] = 1; // пока не придёт Reject, сверка не начнётся снова
                    }
                    sh.set_phase("repairing");
                }
            }
        }
    }
}

/// Считает хеши того, что уже лежит в папке назначения. Совпавшие блоки не придётся передавать.
fn seed_thread(m: Arc<Manifest>, root: PathBuf, out: mpsc::Sender<(u32, Option<[u8; 32]>)>) {
    let files = m.files();
    let mut cache: FileCache = FileCache::new(32);
    let mut buf = vec![0u8; BLOCK];
    for b in 0..m.blocks() {
        let len = m.block_len(b);
        buf[..len].fill(0);
        let r: io::Result<()> = for_ranges(&files, b as u64 * BLOCK as u64, len, |e, off, pos, n| {
            let f = cache.get(&e.path, || {
                let f = File::open(root.join(safe_rel(&e.path).ok_or_else(|| io::Error::other("путь"))?))?;
                no_cache(&f);
                Ok(f)
            })?;
            f.read_exact_at(&mut buf[pos..pos + n], off)
        });
        let h = r.ok().map(|_| *blake3::hash(&buf[..len]).as_bytes());
        if out.send((b, h)).is_err() {
            return;
        }
    }
}

fn seed_journal(ch: &mut Channel, m: &Arc<Manifest>, root: &Path, journal: &Path, sh: &Arc<Shared>, rb: &mut RecvBatch) -> Result<(), Outcome> {
    sh.set_phase("seeding");
    sh.phase_total.store(m.total, Relaxed);
    let (tx, rx) = mpsc::channel();
    let (mm, rr) = (m.clone(), root.to_path_buf());
    std::thread::Builder::new().name("dd-seed".into()).spawn(move || seed_thread(mm, rr, tx)).map_err(io_fail)?;
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).mode(0o600).open(journal).map_err(io_fail)?;
    let (mut done, mut last, mut inbox) = (0u64, Instant::now(), vec![]);
    loop {
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok((b, h)) => {
                done += m.block_len(b) as u64;
                if let Some(h) = h {
                    let mut rec = [0u8; 36];
                    rec[..4].copy_from_slice(&b.to_le_bytes());
                    rec[4..].copy_from_slice(&h);
                    f.write_all(&rec).map_err(io_fail)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }
        if last.elapsed() > Duration::from_millis(250) {
            last = Instant::now();
            sh.phase_done.store(done, Relaxed);
            ch.push_ctrl(&Ctrl::Phase { phase: "seeding".into(), done, total: m.total });
            let _ = ch.pump_ctrl();
            drain_rx(ch, rb, &mut inbox, |_, _, _| {});
            for msg in inbox.drain(..) {
                if let Ctrl::Cancel { reason } = msg {
                    return Err(Outcome::Cancelled(reason));
                }
            }
            if ch.last_rx.elapsed() > LINK_TIMEOUT {
                return Err(Outcome::LinkLost);
            }
            if sh.cancel.load(Relaxed) {
                ch.push_ctrl(&Ctrl::Cancel { reason: "отменено получателем".into() });
                let _ = flush_ctrl(ch, rb);
                return Err(Outcome::Cancelled("отменено".into()));
            }
        }
    }
}

fn prepare_part(m: &Manifest, root: &Path, in_place: bool) -> io::Result<()> {
    fs::create_dir_all(root)?;
    for e in &m.entries {
        let rel = safe_rel(&e.path).ok_or_else(|| io::Error::other("небезопасный путь"))?;
        match e.kind {
            'd' => fs::create_dir_all(root.join(&rel))?,
            'f' if e.size >= (64 << 20) => {
                let f = open_part(root, e)?;
                preallocate(&f, e.size)?;
            }
            // На месте размеры выставляем сразу: старые файлы могли быть длиннее.
            'f' if in_place => {
                open_part(root, e)?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn preallocate(f: &File, size: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let r = unsafe { libc::posix_fallocate(f.as_raw_fd(), 0, size as i64) };
    if r == libc::ENOSPC {
        return Err(io::Error::from_raw_os_error(r));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn preallocate(_: &File, _: u64) -> io::Result<()> {
    Ok(())
}
