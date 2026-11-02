//! Подбор скорости. Модель как у BBR: скорость следует за измеренной пропускной способностью,
//! а потери в эфире Wi-Fi сами по себе не считаются перегрузкой.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const START_RATE: f64 = 20e6;
const MIN_RATE: f64 = 2e6;
const SAMPLE_STARTUP: Duration = Duration::from_millis(50);
// Wi-Fi отдаёт данные пачками, поэтому короткие замеры завышают скорость канала.
const SAMPLE_CRUISE: Duration = Duration::from_millis(250);
const BW_WINDOW: Duration = Duration::from_secs(3);
const CYCLE: [f64; 8] = [1.12, 0.92, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
const CYCLE_STEP: Duration = Duration::from_millis(250);

#[derive(PartialEq, Clone, Copy, Debug)]
enum Mode {
    Startup,
    Cruise,
}

pub struct Cc {
    mode: Mode,
    pub rate: f64,
    bw: VecDeque<(Instant, f64)>,
    min_rtt_us: f64,
    pub srtt_us: f64,
    last_sample: Option<(Instant, u64, u64, u64)>,
    flat_rounds: u32,
    best_startup: f64,
    phase: usize,
    phase_at: Instant,
    rate_before_idle: f64,
}

impl Cc {
    pub fn new() -> Self {
        Self {
            mode: Mode::Startup,
            rate: START_RATE,
            bw: VecDeque::new(),
            min_rtt_us: f64::MAX,
            srtt_us: 0.0,
            last_sample: None,
            flat_rounds: 0,
            best_startup: 0.0,
            phase: 0,
            phase_at: Instant::now(),
            rate_before_idle: START_RATE,
        }
    }

    pub fn on_rtt(&mut self, rtt_us: f64) {
        if rtt_us <= 0.0 {
            return;
        }
        self.min_rtt_us = self.min_rtt_us.min(rtt_us);
        self.srtt_us = if self.srtt_us == 0.0 { rtt_us } else { self.srtt_us * 0.9 + rtt_us * 0.1 };
    }

    fn max_bw(&self) -> f64 {
        self.bw.iter().map(|x| x.1).fold(0.0, f64::max)
    }

    fn queue_us(&self) -> f64 {
        if self.min_rtt_us == f64::MAX {
            0.0
        } else {
            (self.srtt_us - self.min_rtt_us).max(0.0)
        }
    }

    /// Обратная связь: сколько всего байт и пакетов дошло, и самый старший номер пакета.
    pub fn on_feedback(&mut self, now: Instant, bytes: u64, pkts: u64, highest_seq: u64) {
        let Some((t0, b0, p0, s0)) = self.last_sample else {
            self.last_sample = Some((now, bytes, pkts, highest_seq));
            return;
        };
        let dt = now.duration_since(t0);
        let need = if self.mode == Mode::Startup { SAMPLE_STARTUP } else { SAMPLE_CRUISE };
        if dt < need {
            return;
        }
        self.last_sample = Some((now, bytes, pkts, highest_seq));
        let bw = (bytes - b0) as f64 / dt.as_secs_f64();
        let got = (pkts - p0) as f64;
        let span = (highest_seq - s0) as f64;
        let loss = if span > 16.0 { ((span - got) / span).clamp(0.0, 1.0) } else { 0.0 };
        self.bw.push_back((now, bw));
        while self.bw.front().is_some_and(|x| now.duration_since(x.0) > BW_WINDOW) {
            self.bw.pop_front();
        }
        match self.mode {
            Mode::Startup => {
                if bw > self.best_startup * 1.1 {
                    self.best_startup = bw;
                    self.flat_rounds = 0;
                } else {
                    self.flat_rounds += 1;
                }
                let saturated = self.flat_rounds >= 4 && bw < self.rate * 0.9;
                if saturated || self.queue_us() > 40_000.0 || loss > 0.25 {
                    self.mode = Mode::Cruise;
                    self.phase = 1;
                    self.phase_at = now;
                    self.rate = self.max_bw().max(MIN_RATE);
                    self.bw.clear();
                } else if bw >= self.rate * 0.7 {
                    self.rate *= 1.3;
                }
            }
            Mode::Cruise => {
                if now.duration_since(self.phase_at) >= CYCLE_STEP {
                    self.phase = (self.phase + 1) % CYCLE.len();
                    self.phase_at = now;
                }
                let mut gain = CYCLE[self.phase];
                if self.queue_us() > 60_000.0 {
                    gain = gain.min(0.95);
                } else if self.queue_us() < 20_000.0 && bw > self.rate * 0.9 {
                    // Канал свободен и успевает: после провала скорости возвращаемся быстро.
                    gain = gain.max(1.25);
                }
                self.rate = (self.max_bw() * gain).max(MIN_RATE);
            }
        }
    }

    /// Предел пакетов в пути: не даёт слать вслепую, если обратная связь пропала.
    pub fn window_pkts(&self) -> u64 {
        let rtt = if self.min_rtt_us == f64::MAX { 40_000.0 } else { self.min_rtt_us.max(40_000.0) };
        let bdp = self.rate * rtt / 1e6 / 1492.0;
        ((bdp * 3.0) as u64).max(8192)
    }

    /// Данные снова пошли после паузы: замеры за время простоя ничего не говорят о канале,
    /// поэтому начинаем с прежней скорости и снова ищем предел.
    pub fn on_resume(&mut self) {
        self.last_sample = None;
        self.bw.clear();
        self.mode = Mode::Startup;
        self.flat_rounds = 0;
        self.best_startup = 0.0;
        self.rate = self.rate_before_idle.max(START_RATE);
    }

    /// Всё, что было в пути, пропало: прежние замеры канала больше не верны.
    pub fn on_stall(&mut self) {
        self.last_sample = None;
        self.bw.clear();
        self.mode = Mode::Startup;
        self.flat_rounds = 0;
        self.best_startup = 0.0;
        self.rate = START_RATE;
    }

    pub fn on_pause(&mut self) {
        self.rate_before_idle = self.rate;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(limit: f64, loss: f64, secs: f64) -> f64 {
        let mut cc = Cc::new();
        let t0 = Instant::now();
        let (mut bytes, mut pkts, mut seq) = (0u64, 0u64, 0u64);
        let mut now = t0;
        let step = Duration::from_millis(5);
        let mut delivered_last_sec = 0.0;
        let total = (secs / step.as_secs_f64()) as usize;
        for i in 0..total {
            now += step;
            let send = cc.rate * step.as_secs_f64();
            let through = send.min(limit * step.as_secs_f64()) * (1.0 - loss);
            bytes += through as u64;
            pkts += (through / 1424.0) as u64;
            seq += (send / 1424.0) as u64;
            cc.on_rtt(5_000.0);
            cc.on_feedback(now, bytes, pkts, seq);
            if i >= total - 200 {
                delivered_last_sec += through;
            }
        }
        delivered_last_sec
    }

    #[test]
    fn recovers_quickly_after_pause() {
        let mut cc = Cc::new();
        let mut now = Instant::now();
        let step = Duration::from_millis(5);
        let (mut b, mut p, mut q) = (0u64, 0u64, 0u64);
        for _ in 0..1200 {
            now += step;
            let through = cc.rate.min(85e6) * step.as_secs_f64();
            b += through as u64;
            p += (through / 1424.0) as u64;
            q += (cc.rate * step.as_secs_f64() / 1424.0) as u64;
            cc.on_rtt(5_000.0);
            cc.on_feedback(now, b, p, q);
        }
        assert!(cc.rate > 60e6, "{}", cc.rate);
        cc.on_pause();
        now += Duration::from_secs(15);
        cc.on_resume();
        assert!(cc.rate > 60e6, "сразу после паузы {}", cc.rate);
    }

    #[test]
    fn finds_link_speed() {
        let got = run(85e6, 0.0, 6.0);
        assert!(got > 85e6 * 0.8, "{got}");
    }

    #[test]
    fn random_loss_does_not_collapse_rate() {
        let got = run(85e6, 0.03, 6.0);
        assert!(got > 85e6 * 0.7, "{got}");
    }

    #[test]
    fn grows_on_fast_link() {
        let got = run(1000e6, 0.0, 6.0);
        assert!(got > 500e6, "{got}");
    }
}
