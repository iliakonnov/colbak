use colbak_lib::cpio::reader::NextItem;
use colbak_lib::fileinfo::Info;
use std::io::Cursor;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn extract_normal() {
    let file: &[u8] = include_bytes!("big_archive.cpio");
    let file = Cursor::new(file);
    let mut reader = colbak_lib::cpio::Reader::new(file);

    let mut buffer = Vec::new();
    match reader.advance().await.unwrap() {
        NextItem::File(f) => {
            buffer.clear();
            let info = f.info();
            reader = f.drain_to(&mut buffer).await.unwrap();
            println!("{:#?}", info);
            assert_eq!(info.size(), Some(16));
            assert_eq!(info.path.as_bytes(), b"tests/archive/even");
            assert_eq!(buffer, b"even_named_file\n");
        }
        NextItem::End(_) => panic!(),
    }

    match reader.advance().await.unwrap() {
        NextItem::File(f) => {
            buffer.clear();
            let info = f.info();
            reader = f.drain_to(&mut buffer).await.unwrap();
            println!("{:#?}", info);
            assert_eq!(info.size(), Some(12));
            assert_eq!(info.path.as_bytes(), b"tests/archive/foobar");
            assert_eq!(buffer, b"Hello world\n");
        }
        NextItem::End(_) => panic!(),
    }

    match reader.advance().await.unwrap() {
        NextItem::File(f) => {
            buffer.clear();
            let info = f.info();
            reader = f.drain_to(&mut buffer).await.unwrap();
            println!("{:#?}", info);
            assert_eq!(info.size(), Some(15));
            assert_eq!(info.path.as_bytes(), b"tests/archive/odd");
            assert_eq!(buffer, b"odd_named_file\n");
        }
        NextItem::End(_) => panic!(),
    }

    match reader.advance().await.unwrap() {
        NextItem::End(end) => {
            assert!(end.files.is_none());
        }
        NextItem::File(_) => panic!(),
    }
}

#[tokio::test]
async fn extract_json() {
    let mut archive = colbak_lib::cpio::Archive::new();
    archive.add(Info::new("tests/archive/even".into()).await.unwrap());
    archive.add(Info::new("tests/archive/foobar".into()).await.unwrap());
    archive.add(Info::new("tests/archive/odd".into()).await.unwrap());
    let mut buffer = Vec::new();
    archive.read().read_to_end(&mut buffer).await.unwrap();

    let file = Cursor::new(buffer);
    let mut reader = colbak_lib::cpio::Reader::new(file);
    let end = loop {
        match reader.advance().await.unwrap() {
            NextItem::File(f) => reader = f.skip().await.unwrap(),
            NextItem::End(end) => break end,
        }
    };
    let files = end.files.unwrap();
    assert_eq!(files.len(), 3);

    assert_eq!(files[0].size(), Some(16));
    assert_eq!(files[0].path.as_bytes(), b"tests/archive/even");

    assert_eq!(files[1].size(), Some(12));
    assert_eq!(files[1].path.as_bytes(), b"tests/archive/foobar");

    assert_eq!(files[2].size(), Some(15));
    assert_eq!(files[2].path.as_bytes(), b"tests/archive/odd");
}
