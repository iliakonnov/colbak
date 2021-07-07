use crate::serde_b64;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::path::PathBuf;

/// Converts any u64 number to exactly 12 ascii symbols.
/// All these symbols can be used in path names on Windows.
fn u64_to_ascii(num: u64) -> [u8; 12] {
    // To fit into 12 bytes we need at least 41 different chars
    // For 11 bytes we need 57, but that is too much.
    let digits = b'0'..=b'9'; // 10
    let upper = b'A'..=b'Z'; // 25
                             // 6 more chars:
    let additional = [b'-', b'+', b'!', b'=', b'_', b'#'];
    // NOTE: Forbidden characters in Windows are: < > : " / \ | ? *

    let alphabet = additional
        .iter()
        .copied()
        .chain(digits)
        .chain(upper)
        .rev()
        .collect::<Vec<u8>>();
    let alphabet_len = alphabet.len() as u64;
    #[allow(clippy::panic)] // XXX: https://github.com/rust-lang/rust-clippy/issues/7433
    {
        assert!(alphabet_len >= 41, "{} symbols is not enough", alphabet_len);
    }
    let mut result = [alphabet[0]; 12];
    let mut idx = 0;
    let mut num = num;
    while num != 0 {
        let rem = num % alphabet_len;
        let div = num / alphabet_len;
        debug_assert!(idx < 12);

        // Remainder can't be a big number.
        #[allow(clippy::cast_possible_truncation)]
        let rem = rem as usize;

        result[idx] = alphabet[rem];
        num = div;
        idx += 1;
    }
    result
}

/// Represents path using bytes with following requirements:
/// - `/` symbol is used as separator. Windows nor ntfs-3g does not allow this character, so it is safe to use.
/// - `.` symbol is encoded as b"." when possible. It is a plain ASCII symbol, so we should not have any problems.
#[derive(Clone, PartialEq, Serialize, Deserialize, Eq)]
pub struct EncodedPath<K: PathKind>(
    #[serde(with = "serde_b64")] Vec<u8>,
    #[serde(skip, default)] std::marker::PhantomData<K>,
);

/// `PathKind` is used to distinguish paths came from local system (file with such path is very likely to exists)
/// and from remote sources, such as archive. This does not make many difference, but helps to find possible problems.
pub trait PathKind: std::fmt::Debug + Clone + PartialEq + Eq + Sized {}

#[allow(unreachable_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Local(!);
impl PathKind for Local {}

#[allow(unreachable_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct External(!);
impl PathKind for External {}

// We do not support exotic platforms, where MAIN_SEPARATOR is not ascii symbol.
static_assertions::const_assert!(std::path::MAIN_SEPARATOR.is_ascii());

impl EncodedPath<Local> {
    #[must_use]
    pub fn from_path(path: PathBuf) -> Self {
        let os = path.into_os_string();
        let vec = os_str_bytes::OsStringBytes::into_raw_vec(os);
        EncodedPath::from_vec(vec).cast()
    }

    /// Converts `EncodedPath` into `PathBuf` with correct separators matching current platform.
    pub fn to_path(&self) -> Result<PathBuf, os_str_bytes::EncodingError> {
        let mut vec = self.0.clone();
        for i in &mut vec {
            if *i == b'/' {
                *i = std::path::MAIN_SEPARATOR as u8;
            }
        }
        os_str_bytes::OsStringBytes::from_raw_vec(vec)
    }
}

impl EncodedPath<External> {
    /// Creates `EncodedPath` from vector, normalizing all separators if needed.
    #[must_use]
    pub fn from_vec(mut vec: Vec<u8>) -> Self {
        let separator = std::path::MAIN_SEPARATOR as u8;
        for i in &mut vec {
            if *i == separator {
                *i = b'/';
            }
        }
        EncodedPath(vec, PhantomData::default())
    }
}

impl<K: PathKind> EncodedPath<K> {
    /// Changes kind of this path. Use with caution.
    #[must_use]
    pub fn cast<T: PathKind>(self) -> EncodedPath<T> {
        EncodedPath(self.0, PhantomData::default())
    }

    /// # Example
    /// ```
    /// # use colbak_lib::path::EncodedPath;
    /// let path = EncodedPath::from_vec(b"foo/bar/baz".to_vec());
    /// assert_eq!(path.as_bytes(), &b"foo/bar/baz"[..]);
    /// ```
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// ```rust
    /// # use colbak_lib::path::EncodedPath;
    /// let path = b"a/b/c/d.txt";
    /// let path = EncodedPath::from_vec(path.to_vec());
    /// let prefixes = path.prefixes();
    /// let expected: Vec<&'static [u8]> = vec![b"", b"a", b"a/b", b"a/b/c"];
    /// assert_eq!(prefixes, expected);
    /// ```
    #[must_use]
    pub fn prefixes(&self) -> Vec<&[u8]> {
        let slash_positions = self
            .0
            .iter()
            .enumerate()
            .filter(|(_, x)| **x == b'/')
            .map(|(idx, _)| idx);
        let mut result = vec![&b""[..]];
        for slash in slash_positions {
            result.push(&self.0[0..slash]);
        }
        result
    }

    /// Crops path to fit it into `max_length` bytes.
    /// Tries to preserve extension and most of filename by replacing tail with hash.
    /// Since resulting string contains hash, it extremely likely won't collide with other cropped path.
    ///
    /// This function is not intended to be used with extremely low limits (recommended at least 25)
    ///
    /// # Examples
    ///
    /// Short path:
    /// ```rust
    /// # use colbak_lib::path::EncodedPath;
    /// let path = EncodedPath::from_vec(b"a/b/c/d".to_vec());
    /// assert_eq!(path.crop_name_to(10_usize).as_ref(), b"a/b/c/d");
    /// ```
    ///
    /// Long path that should be cropped, but limit is extremely low.
    /// ```should_panic
    /// # use colbak_lib::path::EncodedPath;
    /// let path = EncodedPath::from_vec(b"foo/bar/baz/spam/eggs".to_vec());
    /// let cropped = path.crop_name_to(10_usize);
    /// ```
    ///
    /// Cropping to 25 symbols:
    /// ```
    /// # use colbak_lib::path::EncodedPath;
    /// let path = EncodedPath::from_vec(b"foo/bar/baz/spam/eggs/very_long_filename".to_vec());
    /// let cropped = path.crop_name_to(25_usize);
    /// assert_eq!(cropped.as_ref(), b"foo/bar/baz/s!MMDDY-P_9VA");
    /// assert_eq!(cropped.len(), 25);
    /// ```
    ///
    /// Extension is preserved when possible:
    /// ```
    /// # use colbak_lib::path::EncodedPath;
    /// let path = EncodedPath::from_vec(b"foo/bar/baz/spam/eggs/with_extension.txt".to_vec());
    /// let cropped = path.crop_name_to(25_usize);
    /// assert_eq!(cropped.as_ref(), b"foo/bar/bV-GK2T=9S8UG.txt");
    /// assert_eq!(cropped.len(), 25);
    /// ```
    #[must_use]
    pub fn crop_name_to<L: Into<usize>>(&self, max_length: L) -> Cow<[u8]> {
        const EXTENSION_LENGTH: usize = 10;
        const HASH_LENGTH: usize = 12;

        let max_length = max_length.into();
        if self.0.len() <= max_length {
            return Cow::Borrowed(&self.0);
        }
        assert!(max_length > HASH_LENGTH + EXTENSION_LENGTH);

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.0.hash(&mut hasher);
        let hash = hasher.finish();

        let hash: [u8; HASH_LENGTH] = u64_to_ascii(hash);
        let ext_start = self.0.len() - EXTENSION_LENGTH;
        let dot = self.0.iter()
            .enumerate()
            .skip(ext_start)
            .rfind(|(_, &x)| x == b'.')
            .map(|(idx, _)| idx)
            .unwrap_or(self.0.len());
        let (name, extension) = self.0.split_at(dot);
        let space_available = max_length - extension.len() - hash.len();
        let name = &name[..space_available];
        let res = name
            .iter()
            .chain(hash.iter())
            .chain(extension.iter())
            .copied()
            .collect::<Vec<u8>>();
        log!(warn: "Cropped name `{}` to `{}`", before=self.escaped(), after=res.escaped());
        Cow::Owned(res)
    }
}

/// This trait is implemented for some `[u8]`-like objects that are likely to contain readable string.
pub trait EscapedString {
    /// Converts self to the string, replacing any invalid characters with `\x??` sequences.
    /// Should be used for displaying purposes only.
    fn escaped(&self) -> Cow<str>;
}

impl<T: PathKind> EscapedString for EncodedPath<T> {
    /// # Example
    /// ```
    /// # use colbak_lib::path::{EncodedPath, EscapedString};
    /// let path = EncodedPath::from_vec(b"Hello \xC3\x28 world".to_vec());
    /// assert_eq!(path.escaped(), "Hello \\xC3( world");
    /// ```
    fn escaped(&self) -> Cow<str> {
        self.0.escaped()
    }
}

impl EscapedString for [u8] {
    /// # Examples
    /// 
    /// Strings are preserved as-is when possible:
    /// ```
    /// # use colbak_lib::path::EscapedString;
    /// assert_eq!(b"Hello world".escaped(), "Hello world");
    /// ```
    /// 
    /// NUL byte don't get escaped, it's a valid character in unicode:
    /// ```
    /// # use colbak_lib::path::EscapedString;
    /// assert_eq!(b"Hello \0 world".escaped(), "Hello \0 world");
    /// ```
    /// 
    /// Some invalid unicode:
    /// ```
    /// # use colbak_lib::path::EscapedString;
    /// assert_eq!(b"Hello \xC3\x28 world!".escaped(), "Hello \\xC3( world!");
    /// assert_eq!(b"Hello \xF4\xBF\xBF\xBF world!".escaped(), "Hello \\xF4\\xBF\\xBF\\xBF world!");
    /// ```
    fn escaped(&self) -> Cow<str> {
        let mut remaining = self;
        let mut result = String::new();
        loop {
            match std::str::from_utf8(remaining) {
                Ok(x) => {
                    return if result.is_empty() {
                        Cow::Borrowed(x)
                    } else {
                        result.push_str(x);
                        Cow::Owned(result)
                    }
                }
                Err(err) => {
                    let (valid, bad) = remaining.split_at(err.valid_up_to());
                    let (bad, rest) = bad.split_at(err.error_len().unwrap_or(0));
                    let valid = unsafe { std::str::from_utf8_unchecked(valid) };
                    remaining = rest;
                    result.push_str(valid);
                    let escaped: String = bad.iter().map(|x| format!("\\x{:02X}", x)).collect();
                    result.push_str(&escaped);
                }
            }
        }
    }
}

impl<P: PathKind> std::fmt::Debug for EncodedPath<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let escaped = self.escaped();
        write!(f, "EncodedPath({})", escaped)
    }
}
