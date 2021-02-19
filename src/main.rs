#![feature(type_alias_impl_trait, backtrace, type_ascription)]
#![feature(macro_attributes_in_derive_output)]
#![cfg_attr(windows, feature(windows_by_handle))]
#![allow(dead_code)]

use std::path::PathBuf;

pub use time::OffsetDateTime as DateTime;

mod cpio;
mod fileext;
mod fileinfo;
mod serde_b64;
mod path;
mod types;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "awsync")]
enum Opt {
    CpioCreate { archive: PathBuf, files: Vec<PathBuf> },
    CpioExtract { archive: PathBuf, destination: PathBuf},
    Tree { target: PathBuf, root: PathBuf },
}

async fn entry_point(opt: Opt) -> Result<(), Box<dyn std::error::Error>> {
    match opt {
        Opt::CpioCreate { archive: dest, files } => {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut archive = cpio::Archive::new();
            for f in files {
                let info = fileinfo::Info::new(f).await?;
                archive.add(info);
            }
            let mut src = archive.read();
            let mut dst = tokio::fs::File::create(dest).await?;
            let mut buf = vec![0; 1024];
            loop {
                let len = src.read(&mut buf).await?;
                if len == 0 {
                    break;
                }
                let slice = &buf[..len];
                dst.write_all(slice).await?;
            }
            Ok(())
        },
        Opt::CpioExtract { archive: src, destination } => {
            use tokio::fs::File;
            use cpio::reader::NextItem;
            let archive = File::open(src).await?;
            let mut reader = cpio::Reader::new(archive);
            let files = loop {
                reader = match reader.advance().await? {
                    NextItem::File(f) => {
                        println!("{:#?}", f.info());
                        f.skip().await?
                    },
                    NextItem::End(e) => break e,
                };
            };
            println!("{:#?}", files);
            Ok(())
        },
        Opt::Tree { target, root } => {
            //let _tree = tree::collect(root, target).await?;
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    if let Err(e) = entry_point(opt).await {
        eprintln!("ERROR!");
        eprintln!("{}", e);

        if let Some(trace) = e.backtrace() {
            eprintln!("\nTRACE:");
            eprintln!("{}", trace);
        } else {
            eprintln!("\nTrace missing :(");
        }
    }
}
