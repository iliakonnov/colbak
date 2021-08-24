#![feature(backtrace)]

use colbak_lib::cpio::reader::NextItem;
use colbak_lib::cpio::Archive;
use colbak_lib::database::{Database, SqlName};
use colbak_lib::fileinfo::{Info, UnspecifiedInfo};
use colbak_lib::path::{EscapedString, Local};
use colbak_lib::stream_hash::stream_hash;
use colbak_lib::types::Checksum;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

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
    },
    /// Reads archive from stdin and lists files
    ListCpio,
    /// Creates a snapshot of specified directory
    CreateSnapshot { database: PathBuf, root: PathBuf },
    /// Computes difference between snapshots
    DiffSnapshot {
        database: PathBuf,
        before: String,
        after: String,
    },
    /// Previews how directory will be grouped into packs
    PreviewPacks {
        database: PathBuf,
        directory: PathBuf,
        min_size: u64,
    },
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
        }
        Opt::ListCpio => {
            let stdin = tokio::io::stdin();
            let mut sink = tokio::io::sink(); // We can't seek stdin.
            let mut archive = colbak_lib::cpio::Reader::new(stdin);
            loop {
                match archive.advance().await? {
                    NextItem::File(file) => {
                        let info = file.info();
                        println!("{:#?}", info);
                        archive = file.drain_to(&mut sink).await?;
                    }
                    NextItem::End(end) => {
                        if let Some(files) = end.files {
                            println!("{:#?}", files)
                        }
                        break Ok(());
                    }
                }
            }
        }
        Opt::UnpackCpio { output } => {
            let stdin = tokio::io::stdin();
            let mut archive = colbak_lib::cpio::Reader::new(stdin);
            let mut hashes = Vec::new();
            loop {
                match archive.advance().await? {
                    NextItem::File(file) => {
                        let mut info = file.info();
                        let path = info.path.clone().cast::<Local>().to_path()?;
                        let dst = output.join(path);
                        println!("Extracting {:?}...", dst);
                        match info.data {
                            UnspecifiedInfo::Dir(_) => {
                                tokio::fs::create_dir(&dst).await?;
                                archive = file.to_void().await?;
                            }
                            UnspecifiedInfo::File(_) => {
                                let output = File::create(dst).await?;
                                let mut hasher = stream_hash(output);
                                archive = file.drain_to(&mut hasher).await?;
                                let hash: Checksum = hasher.finalize().into();
                                info.hash = Some(hash);
                            }
                            UnspecifiedInfo::Unknown(_) => {
                                println!("\tSkipping unknown file.");
                                archive = file.to_void().await?;
                            }
                        }
                        hashes.push(info);
                    }
                    NextItem::End(end) => {
                        let files = match end.files {
                            Some(files) => files,
                            None => break Ok(()),
                        };
                        for (expected, found) in files.into_iter().zip(hashes.into_iter()) {
                            let total_match = expected == found;
                            if total_match {
                                continue;
                            }
                            if expected.path != found.path {
                                eprintln!(
                                    "Warning: path mismatch. Expected {:?}, found {:?}",
                                    expected.path, found.path
                                );
                            }
                            let hash_match = expected
                                .hash
                                .zip(found.hash)
                                .map(|(x, y)| x == y)
                                .unwrap_or(true);
                            if !hash_match {
                                eprintln!(
                                    "Warning: hash mismatch at {:?}. Expected {:?}, found {:?}",
                                    found.path, expected.hash, found.hash
                                );
                            }
                        }
                        break Ok(());
                    }
                }
            }
        }
        Opt::CreateSnapshot { database, root } => {
            let mut database = colbak_lib::database::Database::open(database)?;
            let name = SqlName::now();
            let mut snapshot = database.open_snapshot(name)?;
            snapshot.filler()?.fill(&root)?.save()?;
            println!("Created snapshot {}", snapshot.name());
            Ok(())
        }
        Opt::DiffSnapshot {
            database,
            before,
            after,
        } => {
            let database = Database::open(database)?;
            let before = database.readonly_snapshot(SqlName::new(before)?)?;
            let after = database.readonly_snapshot(SqlName::new(after)?)?;
            let diff = database.compare_snapshots(&before, &after)?;
            diff.query().for_each::<_, Infallible>(|row| {
                println!("{:#?}", row);
                Ok(())
            })??;
            Ok(())
        }
        Opt::PreviewPacks {
            database,
            directory,
            min_size,
        } => {
            let mut database = Database::open(database)?;

            let after = {
                let mut after = database.open_snapshot(SqlName::now())?;
                after.filler()?.fill(&directory)?.save()?;
                after.into_name()
            };
            let after = database.readonly_snapshot(after)?;

            let before = database.empty_snapshot()?;
            let diff = database.compare_snapshots(&before, &after)?;

            let packed = colbak_lib::packer::pack(&diff, min_size)?;
            for (n, pack) in packed.0.iter().enumerate() {
                println!("PACK {}:", n + 1);
                for file in pack {
                    let file = diff
                        .query()
                        .by_rowid(*file)?
                        .map(|row| row.path().escaped().into_owned());
                    println!("    {:?}", file);
                }
            }
            Ok(())
        }
    }
}

fn show_bt(err: &dyn StdError) {
    eprintln!("# {}", err);
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
