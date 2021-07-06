#![feature(backtrace)]

use colbak_lib::cpio::reader::NextItem;
use colbak_lib::fileinfo::Info;
use std::error::Error as StdError;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use colbak_lib::cpio::Archive;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "colbak")]
enum Opt {
    /// Reads list of files from stdin and output archive into stdout.
    CreateCpio,
    /// Reads archive from stdin and extracts files
    UnpackCpio {
        /// Where extracted files will be located.
        output: PathBuf,
        /// Compute checksums and check them with ones stored in the archive.
        #[structopt(short, long)]
        check_hashes: bool,
    },
    /// Reads archive from stdin and lists files
    ListCpio,
}

async fn entry_point(opt: Opt) -> Result<(), Box<dyn StdError>> {
    match opt {
        Opt::CreateCpio => {
            let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            let mut archive = Archive::new();
            while let Some(line) = stdin.next_line().await? {
                let path = PathBuf::from(line);
                let info = Info::new(path).await?;
                archive.add(info);
            }
            let mut stdout = tokio::io::stdout();
            let mut reader = archive.read();
            let mut buffer = vec![0; 8 * 1024];
            loop {
                buffer.clear();
                let len = reader.read_buf(&mut buffer).await?;
                if len == 0 {
                    break Ok(());
                }
                stdout.write_all_buf(&mut Cursor::new(&mut buffer)).await?;
            }
        },
        Opt::ListCpio => {
            let stdin = tokio::io::stdin();
            let mut sink = tokio::io::sink();  // We can't seek stdin.
            let mut archive = colbak_lib::cpio::Reader::new(stdin);
            loop {
                match archive.advance().await? {
                    NextItem::File(file) => {
                        let info = file.info();
                        println!("{:#?}", info);
                        archive = file.drain_to(&mut sink).await?;
                    },
                    NextItem::End(end) => {
                        if let Some(files) = end.files {
                            println!("{:#?}", files)
                        }
                        break Ok(());
                    },
                }
            }
        },
        _ => todo!(),
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
        show_bt(e.as_ref());
    }
}
