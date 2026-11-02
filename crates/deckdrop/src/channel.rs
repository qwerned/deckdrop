//! Зашифрованный канал поверх подключённого UDP-сокета и надёжные служебные сообщения.
use crate::crypto::{DirKey, TAG_LEN};
use ddnet::UdpSock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::time::{Duration, Instant};

pub const HDR: usize = 24;
pub const T_DATA: u8 = 1;
pub const T_FEEDBACK: u8 = 2;
pub const T_CTRL: u8 = 3;
pub const T_BEAT: u8 = 4;
pub const T_CTRLACK: u8 = 5;
pub const F_RETX: u8 = 1;
pub const MAX_PKT: usize = 1464;

#[derive(Clone, Copy, Debug)]
pub struct Header {
    pub ty: u8,
    pub flags: u8,
    pub session: u32,
    pub seq: u64,
    pub chunk: u32,
    pub ts: u32,
}

impl Header {
    pub fn write(&self, b: &mut [u8]) {
        b[0] = self.ty;
        b[1] = self.flags;
        b[2..6].copy_from_slice(&self.session.to_le_bytes());
        b[6..8].fill(0);
        b[8..16].copy_from_slice(&self.seq.to_le_bytes());
        b[16..20].copy_from_slice(&self.chunk.to_le_bytes());
        b[20..24].copy_from_slice(&self.ts.to_le_bytes());
    }

    pub fn read(b: &[u8]) -> Option<Self> {
        if b.len() < HDR + TAG_LEN {
            return None;
        }
        Some(Self {
            ty: b[0],
            flags: b[1],
            session: u32::from_le_bytes(b[2..6].try_into().ok()?),
            seq: u64::from_le_bytes(b[8..16].try_into().ok()?),
            chunk: u32::from_le_bytes(b[16..20].try_into().ok()?),
            ts: u32::from_le_bytes(b[20..24].try_into().ok()?),
        })
    }
}

/// Служебные сообщения передачи.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Ctrl {
    Offer {
        job: String,
        name: String,
        items: Vec<crate::stream::Item>,
        total: u64,
        dest: Option<String>,
        overwrite: bool,
        from_llm: bool,
        #[serde(default)]
        in_place: bool,
    },
    Accept {
        exclude: Vec<String>,
        dest: String,
    },
    Decline {
        reason: String,
    },
    Manifest {
        manifest: crate::stream::Manifest,
    },
    /// Ответ получателя: какие блоки уже есть (номер и хеш).
    Ready {
        done: Vec<(u32, String)>,
    },
    Pause,
    Resume,
    Cancel {
        reason: String,
    },
    /// Отправитель: блок не совпал с хешем, нужно принять заново.
    Reject {
        block: u32,
    },
    /// Получатель: эти блоки не прошли сверку с диска.
    Repair {
        blocks: Vec<u32>,
    },
    /// Получатель: фаза (сверка, сборка), для показа на стороне отправителя.
    Phase {
        phase: String,
        done: u64,
        total: u64,
    },
    Verify {
        root: String,
        repaired: u32,
    },
    VerifyResult {
        ok: bool,
        root: String,
    },
    Finished,
    Error {
        message: String,
    },
}

pub struct Channel {
    pub sock: UdpSock,
    pub tx_key: DirKey,
    pub rx_key: DirKey,
    pub session: u32,
    pub seq: u64,
    pub epoch: Instant,
    // надёжные служебные сообщения
    next_frame: u32,
    unacked: VecDeque<(u32, Vec<u8>, Instant)>,
    queued: VecDeque<(u32, Vec<u8>)>,
    rx_next: u32,
    rx_buf: BTreeMap<u32, Vec<u8>>,
    rx_msg: Vec<u8>,
    pub last_rx: Instant,
}

const FRAG: usize = 1200;
const CTRL_WINDOW: usize = 128;

impl Channel {
    pub fn new(sock: UdpSock, tx_key: DirKey, rx_key: DirKey, session: u32) -> Self {
        Self {
            sock,
            tx_key,
            rx_key,
            session,
            seq: 1,
            epoch: Instant::now(),
            next_frame: 0,
            unacked: VecDeque::new(),
            queued: VecDeque::new(),
            rx_next: 0,
            rx_buf: BTreeMap::new(),
            rx_msg: vec![],
            last_rx: Instant::now(),
        }
    }

    pub fn now_us(&self) -> u32 {
        self.epoch.elapsed().as_micros() as u32
    }

    /// Собирает зашифрованный пакет в `out` и возвращает его длину.
    pub fn seal_into(&mut self, ty: u8, flags: u8, chunk: u32, body: &[u8], out: &mut [u8]) -> usize {
        let seq = self.seq;
        self.seq += 1;
        let h = Header { ty, flags, session: self.session, seq, chunk, ts: self.now_us() };
        h.write(&mut out[..HDR]);
        let n = HDR + body.len();
        out[HDR..n].copy_from_slice(body);
        let (a, b) = out.split_at_mut(HDR);
        let tag = self.tx_key.seal(seq, a, &mut b[..body.len()]);
        out[n..n + TAG_LEN].copy_from_slice(&tag);
        n + TAG_LEN
    }

    pub fn send_plain(&mut self, ty: u8, body: &[u8]) -> io::Result<()> {
        let mut b = [0u8; MAX_PKT];
        let n = self.seal_into(ty, 0, 0, body, &mut b);
        match self.sock.send(&b[..n]) {
            Ok(_) => Ok(()),
            Err(e) if ddnet::is_transient_send_error(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Расшифровывает пакет на месте. Возвращает заголовок и длину тела.
    pub fn open(&self, pkt: &mut [u8]) -> Option<(Header, usize)> {
        let h = Header::read(pkt)?;
        if h.session != self.session {
            return None;
        }
        let (a, b) = pkt.split_at_mut(HDR);
        let n = self.rx_key.open(h.seq, a, b)?;
        Some((h, n))
    }

    pub fn push_ctrl(&mut self, msg: &Ctrl) {
        let data = serde_json::to_vec(msg).expect("сообщение");
        let parts: Vec<&[u8]> = data.chunks(FRAG).collect();
        let last = parts.len().saturating_sub(1);
        for (i, p) in parts.iter().enumerate() {
            let mut f = Vec::with_capacity(p.len() + 5);
            f.extend_from_slice(&self.next_frame.to_le_bytes());
            f.push(u8::from(i == last));
            f.extend_from_slice(p);
            self.queued.push_back((self.next_frame, f));
            self.next_frame += 1;
        }
    }

    pub fn ctrl_pending(&self) -> bool {
        !self.unacked.is_empty() || !self.queued.is_empty()
    }

    /// Шлёт новые и просроченные кадры служебных сообщений.
    pub fn pump_ctrl(&mut self) -> io::Result<()> {
        let now = Instant::now();
        while self.unacked.len() < CTRL_WINDOW {
            let Some((id, f)) = self.queued.pop_front() else { break };
            self.send_plain(T_CTRL, &f)?;
            self.unacked.push_back((id, f, now));
        }
        for i in 0..self.unacked.len() {
            if now.duration_since(self.unacked[i].2) > Duration::from_millis(40) {
                let f = std::mem::take(&mut self.unacked[i].1);
                let r = self.send_plain(T_CTRL, &f);
                self.unacked[i].1 = f;
                self.unacked[i].2 = now;
                r?;
            }
        }
        Ok(())
    }

    fn on_ack(&mut self, upto: u32) {
        while self.unacked.front().is_some_and(|x| x.0 < upto) {
            self.unacked.pop_front();
        }
    }

    /// Обрабатывает служебный пакет. Возвращает готовые сообщения.
    pub fn on_ctrl_packet(&mut self, ty: u8, body: &[u8], out: &mut Vec<Ctrl>) -> io::Result<()> {
        if ty == T_CTRLACK {
            if body.len() >= 4 {
                self.on_ack(u32::from_le_bytes(body[..4].try_into().unwrap()));
            }
            return Ok(());
        }
        if body.len() < 5 {
            return Ok(());
        }
        let id = u32::from_le_bytes(body[..4].try_into().unwrap());
        if id >= self.rx_next {
            self.rx_buf.entry(id).or_insert_with(|| body[4..].to_vec());
        }
        while let Some(f) = self.rx_buf.remove(&self.rx_next) {
            self.rx_next += 1;
            self.rx_msg.extend_from_slice(&f[1..]);
            if f[0] == 1 {
                match serde_json::from_slice::<Ctrl>(&self.rx_msg) {
                    Ok(m) => out.push(m),
                    Err(e) => eprintln!("deckdrop: не разобрал служебное сообщение: {e}"),
                }
                self.rx_msg.clear();
            }
        }
        let ack = self.rx_next.to_le_bytes();
        self.send_plain(T_CTRLACK, &ack)
    }
}

/// Тело обратной связи получателя.
pub struct Feedback {
    pub bytes: u64,
    pub pkts: u64,
    pub highest_seq: u64,
    pub echo_ts: u32,
    pub echo_age_us: u32,
    pub credit: u32,
    pub done: Vec<(u32, [u8; 32])>,
    pub nacks: Vec<(u32, u16)>,
}

impl Feedback {
    pub const MAX_DONE: usize = 6;
    pub const MAX_NACKS: usize = 180;

    pub fn encode(&self, b: &mut Vec<u8>) {
        b.clear();
        b.extend_from_slice(&self.bytes.to_le_bytes());
        b.extend_from_slice(&self.pkts.to_le_bytes());
        b.extend_from_slice(&self.highest_seq.to_le_bytes());
        b.extend_from_slice(&self.echo_ts.to_le_bytes());
        b.extend_from_slice(&self.echo_age_us.to_le_bytes());
        b.extend_from_slice(&self.credit.to_le_bytes());
        b.push(self.done.len() as u8);
        for (n, h) in &self.done {
            b.extend_from_slice(&n.to_le_bytes());
            b.extend_from_slice(h);
        }
        b.extend_from_slice(&(self.nacks.len() as u16).to_le_bytes());
        for (s, l) in &self.nacks {
            b.extend_from_slice(&s.to_le_bytes());
            b.extend_from_slice(&l.to_le_bytes());
        }
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        let mut r = Reader(b);
        let (bytes, pkts, highest_seq) = (r.u64()?, r.u64()?, r.u64()?);
        let (echo_ts, echo_age_us, credit) = (r.u32()?, r.u32()?, r.u32()?);
        let nd = r.u8()? as usize;
        let mut done = Vec::with_capacity(nd);
        for _ in 0..nd {
            let n = r.u32()?;
            done.push((n, r.take(32)?.try_into().ok()?));
        }
        let nn = r.u16()? as usize;
        let mut nacks = Vec::with_capacity(nn);
        for _ in 0..nn {
            nacks.push((r.u32()?, r.u16()?));
        }
        Some(Self { bytes, pkts, highest_seq, echo_ts, echo_age_us, credit, done, nacks })
    }
}

/// Сигнал отправителя: докуда дошла первая передача и какие хеши блоков подтверждены.
pub struct Beat {
    pub next_first_chunk: u32,
    pub paused: bool,
    pub confirmed: Vec<u32>,
}

impl Beat {
    pub const MAX_CONFIRMED: usize = 64;

    pub fn encode(&self, b: &mut Vec<u8>) {
        b.clear();
        b.extend_from_slice(&self.next_first_chunk.to_le_bytes());
        b.push(u8::from(self.paused));
        b.push(self.confirmed.len() as u8);
        for c in &self.confirmed {
            b.extend_from_slice(&c.to_le_bytes());
        }
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        let mut r = Reader(b);
        let next_first_chunk = r.u32()?;
        let paused = r.u8()? != 0;
        let n = r.u8()? as usize;
        let mut confirmed = Vec::with_capacity(n);
        for _ in 0..n {
            confirmed.push(r.u32()?);
        }
        Some(Self { next_first_chunk, paused, confirmed })
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feedback_roundtrip_fits_packet() {
        let f = Feedback {
            bytes: 1,
            pkts: 2,
            highest_seq: 3,
            echo_ts: 4,
            echo_age_us: 5,
            credit: 6,
            done: vec![(7, [9u8; 32]); Feedback::MAX_DONE],
            nacks: vec![(8, 1); Feedback::MAX_NACKS],
        };
        let mut b = vec![];
        f.encode(&mut b);
        assert!(b.len() <= crate::stream::CHUNK);
        let g = Feedback::decode(&b).unwrap();
        assert_eq!(g.done.len(), Feedback::MAX_DONE);
        assert_eq!(g.nacks.len(), Feedback::MAX_NACKS);
        assert_eq!(g.credit, 6);
    }
}
