#![feature(backtrace)]

use colbak_lib::*;
use std::error::Error as StdError;
use std::path::PathBuf;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "colbak")]
enum Opt {
    CpioCreate {
        archive: PathBuf,
        files: Vec<PathBuf>,
    },
    CpioExtract {
        archive: PathBuf,
        destination: PathBuf,
    },
    Snapshot {
        db: PathBuf,
        root: PathBuf,
    },
    Compare {
        db: PathBuf,
        before: String,
        after: String,
    },
}

async fn entry_point(opt: Opt) -> Result<(), TopError> {
    match opt {
        Opt::CpioCreate {
            archive: dest,
            files,
        } => create_cpio(dest, files).await,
        Opt::CpioExtract {
            archive,
            destination,
        } => extract_cpio(archive, destination).await,
        Opt::Snapshot { db, root } => create_snapshot(db, &root),
        Opt::Compare { db, before, after } => diff_snapshot(db, before, after),
    }
}

fn show_bt(err: &dyn StdError) {
    println!("# {}", err);
    match err.backtrace() {
        Some(trace) => eprintln!("{}", trace),
        None => eprintln!("\nTrace missing :("),
    }

    if let Some(source) = err.source() {
        eprintln!("\nCaused by:");
        show_bt(source);
    }
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();
    if let Err(e) = entry_point(opt).await {
        eprintln!("ERROR!");
        show_bt(&e);
    }
}
