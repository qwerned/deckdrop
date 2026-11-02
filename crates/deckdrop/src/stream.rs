//! Манифест передачи: все файлы склеены в один логический поток, поделённый на блоки и куски.
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

/// Полезная нагрузка одного пакета. 1424 + заголовок 24 + метка 16 + IP/UDP 28 = 1492 байта.
pub const CHUNK: usize = 1424;
pub const BLOCK_CHUNKS: usize = 2048;
pub const BLOCK: usize = CHUNK * BLOCK_CHUNKS;
const ALIGN_FROM: u64 = 64 << 20;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Entry {
    /// Относительный путь с разделителем `/`.
    pub path: String,
    /// f — файл, d — папка, l — символическая ссылка.
    pub kind: char,
    pub size: u64,
    pub mtime_s: i64,
    pub mtime_ns: u32,
    pub mode: u32,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub target: Option<String>,
    /// Смещение в логическом потоке (только для файлов).
    pub offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub entries: Vec<Entry>,
    pub total: u64,
}

impl Manifest {
    pub fn blocks(&self) -> u32 {
        (self.total.div_ceil(BLOCK as u64)) as u32
    }

    pub fn block_len(&self, b: u32) -> usize {
        let start = b as u64 * BLOCK as u64;
        (self.total - start).min(BLOCK as u64) as usize
    }

    pub fn id(&self) -> String {
        let mut h = blake3::Hasher::new();
        h.update(serde_json::to_string(self).expect("манифест").as_bytes());
        crate::crypto::hex(&h.finalize().as_bytes()[..12])
    }

    /// Индексы записей-файлов, отсортированные по смещению. Используется вместе с `locate`.
    pub fn files(&self) -> Vec<&Entry> {
        self.entries.iter().filter(|e| e.kind == 'f').collect()
    }
}

/// Находит куски потока [offset, offset+len) в файлах. Записи файлов идут по возрастанию смещения.
pub fn for_ranges<'a, E>(files: &[&'a Entry], offset: u64, len: usize, mut f: impl FnMut(&'a Entry, u64, usize, usize) -> Result<(), E>) -> Result<(), E> {
    let end = offset + len as u64;
    let mut i = files.partition_point(|e| e.offset + e.size <= offset);
    while i < files.len() && files[i].offset < end {
        let e = files[i];
        if e.size > 0 {
            let s = offset.max(e.offset);
            let t = end.min(e.offset + e.size);
            if t > s {
                f(e, s - e.offset, (s - offset) as usize, (t - s) as usize)?;
            }
        }
        i += 1;
    }
    Ok(())
}

fn add_path(entries: &mut Vec<Entry>, disk: &Path, rel: String, offset: &mut u64, sources: &mut Vec<(String, PathBuf)>) -> io::Result<()> {
    let md = fs::symlink_metadata(disk)?;
    let ft = md.file_type();
    let mut e = Entry {
        path: rel.clone(),
        kind: 'f',
        size: 0,
        mtime_s: md.mtime(),
        mtime_ns: md.mtime_nsec() as u32,
        mode: md.permissions().mode() & 0o777,
        target: None,
        offset: 0,
    };
    if ft.is_symlink() {
        e.kind = 'l';
        e.target = Some(fs::read_link(disk)?.to_string_lossy().into_owned());
        entries.push(e);
    } else if ft.is_dir() {
        e.kind = 'd';
        entries.push(e);
        let mut kids: Vec<_> = fs::read_dir(disk)?.collect::<Result<_, _>>()?;
        kids.sort_by_key(|k| k.file_name());
        for k in kids {
            let name = k.file_name().to_string_lossy().into_owned();
            if name == ".DS_Store" {
                continue;
            }
            add_path(entries, &k.path(), format!("{rel}/{name}"), offset, sources)?;
        }
    } else if ft.is_file() {
        e.size = md.len();
        // Большой файл начинается с границы блока: тогда его начало совпадёт со старой версией,
        // даже если файлы перед ним изменили размер.
        if e.size >= ALIGN_FROM {
            *offset = offset.next_multiple_of(BLOCK as u64);
        }
        e.offset = *offset;
        *offset += md.len();
        sources.push((rel, disk.to_path_buf()));
        entries.push(e);
    }
    Ok(())
}

pub struct Scan {
    pub manifest: Manifest,
    /// Относительный путь → файл на диске отправителя.
    pub sources: std::collections::HashMap<String, PathBuf>,
    /// Верхние элементы: имя, число файлов, размер.
    pub items: Vec<Item>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub name: String,
    pub files: u64,
    pub size: u64,
}

pub fn scan(paths: &[PathBuf], exclude: &[String]) -> io::Result<Scan> {
    let mut entries = vec![];
    let mut sources = vec![];
    let mut offset = 0u64;
    let mut items = vec![];
    for p in paths {
        let name = p.file_name().ok_or_else(|| io::Error::other(format!("нет имени у {}", p.display())))?.to_string_lossy().into_owned();
        if exclude.contains(&name) {
            continue;
        }
        let oldlen = entries.len();
        add_path(&mut entries, p, name.clone(), &mut offset, &mut sources)?;
        let added: Vec<&Entry> = entries[oldlen..].iter().filter(|e| e.kind == 'f').collect();
        items.push(Item { name, files: added.len() as u64, size: added.iter().map(|e| e.size).sum() });
    }
    Ok(Scan { manifest: Manifest { entries, total: offset }, sources: sources.into_iter().collect(), items })
}

/// Проверяет путь из манифеста: он должен оставаться внутри папки назначения.
pub fn safe_rel(path: &str) -> Option<PathBuf> {
    if path.is_empty() || path.contains('\0') {
        return None;
    }
    let p = Path::new(path);
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Normal(x) => out.push(x),
            _ => return None,
        }
    }
    Some(out)
}

/// Ссылка допустима, только если остаётся внутри передаваемого дерева.
pub fn safe_symlink(link_rel: &str, target: &str) -> bool {
    let t = Path::new(target);
    if t.is_absolute() {
        return false;
    }
    let mut depth: i64 = Path::new(link_rel).components().count() as i64 - 1;
    for c in t.components() {
        match c {
            Component::ParentDir => depth -= 1,
            Component::Normal(_) => depth += 1,
            Component::CurDir => {}
            _ => return false,
        }
        if depth < 0 {
            return false;
        }
    }
    true
}

pub fn validate(m: &Manifest) -> Result<(), String> {
    let mut expect = 0u64;
    for e in &m.entries {
        if safe_rel(&e.path).is_none() {
            return Err(format!("небезопасный путь: {}", e.path));
        }
        if !matches!(e.kind, 'f' | 'd' | 'l') {
            return Err(format!("неизвестный тип записи: {}", e.path));
        }
        if e.kind == 'f' {
            if e.offset < expect || e.offset - expect >= BLOCK as u64 {
                return Err(format!("неверное смещение: {}", e.path));
            }
            expect = e.offset + e.size;
        }
    }
    if expect != m.total {
        return Err("сумма размеров не совпадает с манифестом".into());
    }
    Ok(())
}

pub struct FileCache<'a> {
    open: std::collections::HashMap<&'a str, File>,
    order: std::collections::VecDeque<&'a str>,
    cap: usize,
}

impl<'a> FileCache<'a> {
    pub fn new(cap: usize) -> Self {
        Self { open: Default::default(), order: Default::default(), cap }
    }

    pub fn get(&mut self, key: &'a str, opener: impl FnOnce() -> io::Result<File>) -> io::Result<&File> {
        if !self.open.contains_key(key) {
            if self.open.len() >= self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.open.remove(old);
                }
            }
            self.open.insert(key, opener()?);
            self.order.push_back(key);
        }
        Ok(&self.open[key])
    }
}

/// Хеш потока: BLAKE3 от последовательности хешей блоков.
pub fn root_hash(hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    for x in hashes {
        h.update(x);
    }
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_escaping_paths() {
        assert!(safe_rel("../x").is_none());
        assert!(safe_rel("/etc/passwd").is_none());
        assert!(safe_rel("a/../../x").is_none());
        assert!(safe_rel("a/b/c").is_some());
    }

    #[test]
    fn symlink_must_stay_inside() {
        assert!(safe_symlink("a/b/link", "../c"));
        assert!(!safe_symlink("a/link", "../../c"));
        assert!(!safe_symlink("a/link", "/etc/passwd"));
    }

    #[test]
    fn ranges_cross_file_boundaries() {
        let mk = |p: &str, off, size| Entry { path: p.into(), kind: 'f', size, mtime_s: 0, mtime_ns: 0, mode: 0, target: None, offset: off };
        let (a, b, c) = (mk("a", 0, 10), mk("empty", 10, 0), mk("c", 10, 5));
        let files = vec![&a, &b, &c];
        let mut got = vec![];
        for_ranges::<()>(&files, 8, 5, |e, off, pos, len| {
            got.push((e.path.clone(), off, pos, len));
            Ok(())
        })
        .unwrap();
        assert_eq!(got, vec![("a".to_string(), 8, 0, 2), ("c".to_string(), 0, 2, 3)]);
    }
}
