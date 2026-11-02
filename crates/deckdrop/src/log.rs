//! Журнал службы: строки с местным временем в `deckdrop.log` рядом с данными службы и в stderr.
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Mutex;

const MAX_LEN: u64 = 4 << 20;
static LOCK: Mutex<()> = Mutex::new(());

pub fn write(msg: &str) {
    let line = format!("{} {msg}\n", now());
    eprint!("{line}");
    let _g = LOCK.lock();
    let dir = crate::node::data_dir();
    let path = dir.join("deckdrop.log");
    // Старый журнал остаётся одной копией: этого хватает, чтобы разобрать последний сбой.
    if fs::metadata(&path).is_ok_and(|m| m.len() > MAX_LEN) {
        let _ = fs::rename(&path, dir.join("deckdrop.log.1"));
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = f.write_all(line.as_bytes());
    }
}

fn now() -> String {
    let t = unsafe { libc::time(std::ptr::null_mut()) };
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe { libc::localtime_r(&t, &mut tm) };
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
}

macro_rules! dlog {
    ($($a:tt)*) => { $crate::log::write(&format!($($a)*)) };
}
pub(crate) use dlog;
