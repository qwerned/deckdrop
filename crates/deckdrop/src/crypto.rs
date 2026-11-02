//! Ключи сессии и сопряжение. Данные шифруются AES-256-GCM (ring).
use ring::aead::{Aad, LessSafeKey, Nonce, Tag, UnboundKey, AES_256_GCM};
use ring::{agreement, hkdf, hmac, rand::SecureRandom, rand::SystemRandom};
use spake2::{Ed25519Group, Identity, Password, Spake2};

pub const TAG_LEN: usize = 16;

pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    SystemRandom::new().fill(&mut b).expect("нет источника случайных чисел");
    b
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

struct KeyLen(usize);
impl hkdf::KeyType for KeyLen {
    fn len(&self) -> usize {
        self.0
    }
}

pub fn hkdf32(secret: &[u8], salt: &[u8], info: &[u8]) -> [u8; 32] {
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, salt).extract(secret);
    let infos = [info];
    let okm = prk.expand(&infos, KeyLen(32)).expect("hkdf");
    let mut out = [0u8; 32];
    okm.fill(&mut out).expect("hkdf");
    out
}

pub fn mac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    let mut c = hmac::Context::with_key(&k);
    for p in parts {
        c.update(p);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(c.sign().as_ref());
    out
}

pub fn mac_ok(key: &[u8], parts: &[&[u8]], got: &[u8]) -> bool {
    let want = mac(key, parts);
    // Сравнение без раннего выхода: время не должно зависеть от совпавшей части.
    want.len() == got.len() && std::hint::black_box(want.iter().zip(got).fold(0u8, |d, (a, b)| d | (a ^ b))) == 0
}

/// Ключ одного направления. Номер пакета служит одноразовым числом, поэтому повторов нет,
/// пока ключ живёт одну сессию и счётчик только растёт.
pub struct DirKey {
    key: LessSafeKey,
    salt: [u8; 4],
}

impl DirKey {
    pub fn new(key: &[u8; 32], salt: [u8; 4]) -> Self {
        Self { key: LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).expect("ключ")), salt }
    }

    fn nonce(&self, seq: u64) -> Nonce {
        let mut n = [0u8; 12];
        n[..4].copy_from_slice(&self.salt);
        n[4..].copy_from_slice(&seq.to_le_bytes());
        Nonce::assume_unique_for_key(n)
    }

    pub fn seal(&self, seq: u64, aad: &[u8], body: &mut [u8]) -> [u8; TAG_LEN] {
        let tag: Tag = self.key.seal_in_place_separate_tag(self.nonce(seq), Aad::from(aad), body).expect("шифрование");
        let mut out = [0u8; TAG_LEN];
        out.copy_from_slice(tag.as_ref());
        out
    }

    /// `body` содержит шифротекст и метку; при успехе возвращает длину открытых данных.
    pub fn open(&self, seq: u64, aad: &[u8], body: &mut [u8]) -> Option<usize> {
        self.key.open_in_place(self.nonce(seq), Aad::from(aad), body).ok().map(|p| p.len())
    }
}

pub struct Ephemeral {
    private: agreement::EphemeralPrivateKey,
    pub public: [u8; 32],
}

pub fn ephemeral() -> Ephemeral {
    let rng = SystemRandom::new();
    let private = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &rng).expect("x25519");
    let pk = private.compute_public_key().expect("x25519");
    let mut public = [0u8; 32];
    public.copy_from_slice(pk.as_ref());
    Ephemeral { private, public }
}

pub fn dh(e: Ephemeral, peer_public: &[u8]) -> Option<[u8; 32]> {
    let peer = agreement::UnparsedPublicKey::new(&agreement::X25519, peer_public);
    agreement::agree_ephemeral(e.private, &peer, |s| {
        let mut o = [0u8; 32];
        o.copy_from_slice(s);
        o
    })
    .ok()
}

/// Ключи сессии: общий ключ пары плюс эфемерный обмен X25519.
pub struct SessionKeys {
    pub init_to_resp: DirKey,
    pub resp_to_init: DirKey,
}

pub fn session_keys(psk: &[u8], shared: &[u8; 32], transcript: &[u8]) -> SessionKeys {
    let mut ikm = Vec::with_capacity(64);
    ikm.extend_from_slice(psk);
    ikm.extend_from_slice(shared);
    let a = hkdf32(&ikm, transcript, b"deckdrop i2r");
    let b = hkdf32(&ikm, transcript, b"deckdrop r2i");
    SessionKeys { init_to_resp: DirKey::new(&a, [1, 0, 0, 0]), resp_to_init: DirKey::new(&b, [2, 0, 0, 0]) }
}

/// Ключ для запечатанных сообщений о состоянии устройства (между сопряжёнными).
pub fn status_key(psk: &[u8]) -> DirKey {
    DirKey::new(&hkdf32(psk, b"", b"deckdrop status"), [3, 0, 0, 0])
}

// ---------- сопряжение: шестизначный код и SPAKE2 ----------

pub struct PairInit(Spake2<Ed25519Group>);

/// Начало обмена: `a_id` показывает код, `b_id` его вводит.
pub fn pair_begin(code: &str, is_a: bool) -> (PairInit, Vec<u8>) {
    let pw = Password::new(code.as_bytes());
    let (ida, idb) = (Identity::new(b"deckdrop-a"), Identity::new(b"deckdrop-b"));
    let (s, msg) = if is_a { Spake2::<Ed25519Group>::start_a(&pw, &ida, &idb) } else { Spake2::<Ed25519Group>::start_b(&pw, &ida, &idb) };
    (PairInit(s), msg)
}

pub fn pair_finish(p: PairInit, peer_msg: &[u8]) -> Option<[u8; 32]> {
    let k = p.0.finish(peer_msg).ok()?;
    Some(hkdf32(&k, b"", b"deckdrop pair"))
}
