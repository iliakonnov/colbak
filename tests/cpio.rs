extern crate colbak_lib;

use std::slice::SliceIndex;

use colbak_lib::fileinfo::Info;
use tokio::io::AsyncReadExt;

// Useful cpio format inspector: https://ide.kaitai.io/#
// Useful command:
// ```bash
// echo "199, 113, 0, 0"
//      | python -c '__import__("sys").stdout.buffer.write(bytes(int(i) for i in input().split(", ")))'
//      | hexdump -C
// ```

const EMPTY: &[u8] = &[
    0xC7, 0x71, // c_magic: 0o070707
    0, 0, // c_dev
    0, 0, // c_ino
    0, 0, // c_mode
    0, 0, // c_gid
    0, 0, // c_uid
    1, 0, // c_nlink
    0, 0, // c_rdev
    0, 0, 0, 0, // c_mtime[2]
    11, 0, // c_namesize: 11
    0, 0, 0, 0, // c_filesize[2]
    b'T', b'R', b'A', b'I', b'L', b'E', b'R', b'!', b'!', b'!', b'\0',
    b'\0', // Additional NUL byte, since filename length is odd
    b'[', b']', // Empty json array
];

fn normalize_header_at<P>(buffer: &mut [u8], expected: &[u8], position: P)
where
    P: SliceIndex<[u8], Output = [u8]> + Clone,
{
    let buffer: &mut [u8] = &mut buffer[position.clone()];
    let expected: &[u8] = &expected[position];

    assert_eq!(&buffer[0..=1], [0xC7, 0x71]);
    assert_eq!(&expected[0..=1], [0xC7, 0x71]);

    // Zero out c_dev. In our format that field stores higher bits of inode.
    (&mut buffer[2..=3]).copy_from_slice(&[0, 0]);

    // Copy user id and gid.
    (&mut buffer[8..=12]).copy_from_slice(&expected[8..=12]);
}

#[tokio::test]
async fn empty() {
    let mut archive = colbak_lib::cpio::Archive::new();
    let mut buffer = Vec::new();
    archive.read().read_to_end(&mut buffer).await.unwrap();
    assert_eq!(buffer, EMPTY);
}

#[tokio::test]
async fn odd_named_file() {
    // echo "tests/archive/odd" | cpio -o --io-size=1 --ignore-devno > tests/odd_named_file.cpio
    const EXPECTED: &[u8] = include_bytes!("odd_named_file.cpio");

    let mut archive = colbak_lib::cpio::Archive::new();
    archive.add(Info::new("tests/archive/odd".into()).await.unwrap());
    let mut buffer = Vec::new();
    archive.read().read_to_end(&mut buffer).await.unwrap();

    // We are not testing json tail here.
    let buffer = &mut buffer[..EXPECTED.len()];
    normalize_header_at(buffer, EXPECTED, ..);

    assert_eq!(buffer, EXPECTED);
}

#[tokio::test]
async fn even_named_file() {
    // echo "tests/archive/even" | cpio -o --io-size=1 --ignore-devno > tests/even_named_file.cpio
    const EXPECTED: &[u8] = include_bytes!("even_named_file.cpio");

    let mut archive = colbak_lib::cpio::Archive::new();
    archive.add(Info::new("tests/archive/even".into()).await.unwrap());
    let mut buffer = Vec::new();
    archive.read().read_to_end(&mut buffer).await.unwrap();

    let buffer = &mut buffer[..EXPECTED.len()];
    normalize_header_at(buffer, EXPECTED, ..);

    assert_eq!(buffer, EXPECTED);
}

#[tokio::test]
async fn big_archive() {
    // find tests/archive | sort | tee /dev/fd/2 | cpio -o --io-size=1 --ignore-devno > tests/big_archive.cpio
    const EXPECTED: &[u8] = include_bytes!("big_archive.cpio");

    let mut archive = colbak_lib::cpio::Archive::new();
    archive.add(Info::new("tests/archive/even".into()).await.unwrap());
    archive.add(Info::new("tests/archive/foobar".into()).await.unwrap());
    archive.add(Info::new("tests/archive/odd".into()).await.unwrap());
    let mut buffer = Vec::new();
    archive.read().read_to_end(&mut buffer).await.unwrap();

    let buffer = &mut buffer[..EXPECTED.len()];
    normalize_header_at(buffer, EXPECTED, 0x00..);
    normalize_header_at(buffer, EXPECTED, 0x3E..);
    normalize_header_at(buffer, EXPECTED, 0x7A..);

    assert_eq!(buffer, EXPECTED);
}
