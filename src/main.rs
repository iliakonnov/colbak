#![feature(backtrace)]

use awsync_lib::*;
use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "awsync")]
enum Opt {
    CpioCreate {
        archive: PathBuf,
        files: Vec<PathBuf>,
    },
    CpioExtract {
        archive: PathBuf,
        destination: PathBuf,
    },
    Tree {
        target: PathBuf,
        root: PathBuf,
    },
}

async fn entry_point(opt: Opt) -> Result<(), Box<dyn std::error::Error>> {
    match opt {
        Opt::CpioCreate {
            archive: dest,
            files,
        } => awsync_lib::create_cpio(dest, files).await,
        Opt::CpioExtract {
            archive: src,
            destination,
        } => extract_cpio(src, destination).await,
        Opt::Tree { target, root } => Ok(()),
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
