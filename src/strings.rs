use std::borrow::Cow;
use std::ffi::OsStr;

fn u32_to_ascii(num: u32) -> [u8; 6] {
    // To fit into 6 bytes we need at least 41 different chars
    // For 5 bytes we need 85, but that is too much.
    let digits = b'0'..b'9'; // 10
    let upper = b'A'..b'Z'; // 25
    // 6 more chars:
    let additional = [b'-', b'+', b'!', b'=', b'_', b'#'];

    let alphabet = additional
        .iter()
        .copied()
        .chain(digits)
        .chain(upper)
        .rev()
        .collect::<Vec<u8>>();
    assert!(alphabet.len() >= 41);
    let mut result = [alphabet[0]; 6];
    let mut idx = 0;
    let mut num = num as usize;
    while num != 0 {
        let rem = num % alphabet.len();
        let div = num / alphabet.len();
        result[idx] = alphabet[rem];
        num = div;
        idx += 1;
    }
    result
}

const EXTRA_SPACE: usize = 128;

pub fn crop_name(name: Cow<[u8]>) -> Cow<[u8]> {
    let max_length = (u16::MAX as usize) - EXTRA_SPACE - 1;
    if name.len() <= max_length {
        return name;
    }

    let hash = xxhrs::XXH32::hash(&name);
    let hash = u32_to_ascii(hash);
    let ext_start = name.len() - 10;
    let dot = (&name[ext_start..])
        .iter()
        .rposition(|&x| x == b'.')
        .unwrap_or(name.len());
    let (name, extension) = name.split_at(dot);
    let space_available = max_length - extension.len() - hash.len();
    let name = &name[..space_available];
    let res = name
        .iter()
        .chain(hash.iter())
        .chain(extension.iter())
        .copied()
        .collect::<Vec<u8>>();
    Cow::Owned(res)
}

pub fn osstr_to_bytes(s: &OsStr) -> Cow<[u8]> {
    match s.to_str() {
        Some(unicode) => str_to_bytes(unicode),
        None => os_str_bytes::OsStrBytes::to_bytes(s),
    }
}

pub fn str_to_bytes(s: &str) -> Cow<[u8]> {
    Cow::Borrowed(s.as_bytes())
}

pub fn bytes_to_osstr(s: &[u8]) -> Result<Cow<OsStr>, os_str_bytes::EncodingError> {
    match std::str::from_utf8(s) {
        Ok(s) => Ok(Cow::Borrowed(OsStr::new(s))),
        Err(_) => {
            use os_str_bytes::OsStrBytes;
            OsStr::from_bytes(s)
        }
    }
}
