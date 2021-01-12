use serde::Serialize;
use snafu::{ResultExt, Snafu};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::db::BEu64;
use super::Tree;

#[derive(Debug, Snafu)]
pub enum JsonizationError {
    #[snafu(context(false))]
    IoFailed { source: tokio::io::Error },
    #[snafu(display("database error: {}", source))]
    DatabaseFailed { source: heed::Error },
    #[snafu(context(false))]
    JsonFailed { source: serde_json::Error },
}

async fn write_section<W, I, T>(dst: &mut W, src: I) -> Result<(), JsonizationError>
where
    W: AsyncWrite + Unpin,
    I: Iterator<Item = heed::Result<(BEu64, T)>>,
    T: Serialize,
{
    let mut is_first = true;
    for i in src {
        if !is_first {
            dst.write_all(b",\n").await?;
        }
        let (k, v) = i.context(DatabaseFailed {})?;
        let file = serde_json::to_string_pretty(&v)?;
        dst.write_all(format!("\"{}\": {}", k.get(), file).as_bytes())
            .await?;
        is_first = false;
    }
    Ok(())
}

impl Tree {
    pub async fn to_json<W: AsyncWrite + Unpin>(
        &self,
        dst: &mut W,
    ) -> Result<(), JsonizationError> {
        let txn = self.env.read_txn().context(DatabaseFailed {})?;
        dst.write_all(b"{\n").await?;

        dst.write_all(b"\"files\": {\n").await?;
        write_section(dst, self.files.iter(&txn).context(DatabaseFailed {})?).await?;

        dst.write_all(b"\n}, \"dirs\": {\n").await?;
        write_section(dst, self.directories.iter(&txn).context(DatabaseFailed {})?).await?;

        dst.write_all(b"\n}, \"others\": {\n").await?;
        write_section(dst, self.others.iter(&txn).context(DatabaseFailed {})?).await?;

        dst.write_all(b"\n}}").await?;
        Ok(())
    }
}
