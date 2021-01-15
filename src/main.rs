#![feature(type_alias_impl_trait, backtrace, type_ascription)]
#![cfg_attr(windows, feature(windows_by_handle))]
#![allow(dead_code)]

use std::path::PathBuf;

pub use time::OffsetDateTime as DateTime;

mod fileinfo;
mod serialization;
mod strings;
mod cpio;
mod tree;
mod fileext;
mod types;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "awsync")]
enum Opt {
    Cpio {
        dest: PathBuf,
        files: Vec<PathBuf>
    }
}


async fn entry_point(opt: Opt) -> Result<(), Box<dyn std::error::Error>> {
    match opt {
        Opt::Cpio { dest, files } => {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut archive = cpio::Archive::new();
            for f in files {
                let info = fileinfo::Info::new(f).await?;
                archive.add(info.path.clone(), info);
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
