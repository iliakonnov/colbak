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
    Snapshot {
        db: PathBuf,
        root: PathBuf,
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
        Opt::Snapshot { db, root } => create_snapshot(db, root).await,
    }
}

#[tokio::main]
async fn main() {
    color_backtrace::install();

    let opt = Opt::from_args();
    if let Err(e) = entry_point(opt).await {
        eprintln!("ERROR!");
        eprintln!("{}", e);

        if let Some(trace) = snafu::ErrorCompat::backtrace(&e) {
            color_backtrace::BacktracePrinter::new()
                .print_trace(trace, &mut color_backtrace::default_output_stream())
                .unwrap();
        } else {
            eprintln!("\nTrace missing :(");
        }
    }
}
