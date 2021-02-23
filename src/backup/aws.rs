use rusoto_s3::{S3, S3Client, AbortMultipartUploadRequest};
use snafu::{Snafu, ResultExt, OptionExt};
use std::future::Future;

struct Aws {
    bucket: String,
    client: S3Client,
    max_parallel: usize,
}

#[derive(Debug, Snafu)]
enum Error {
    FailedMany {
        errors: Vec<Error>
    },
    NotChargedButShould {
        backtrace: snafu::Backtrace
    },
    MissingRequiredFields {
        backtrace: snafu::Backtrace
    },
    ListUploadsFailed {
        source: rusoto_core::RusotoError<rusoto_s3::ListMultipartUploadsError>,
    },
    AbortUploadFailed {
        source: rusoto_core::RusotoError<rusoto_s3::AbortMultipartUploadError>,
    },
}

impl Aws {
    async fn do_many_requests<I>(&self, iter: I) -> Vec<<I::Item as Future>::Output> where
        I: IntoIterator,
        I::Item: Future,
    {
        use futures::stream::StreamExt;
        futures::stream::iter(iter)
            .buffer_unordered(self.max_parallel)
            .collect()
            .await
    }

    async fn abort_upload(&self, upload: rusoto_s3::MultipartUpload) -> Result<(), Error> {
        let result = self.client.abort_multipart_upload(AbortMultipartUploadRequest {
            bucket: self.bucket.to_string(),
            key: upload.key.context(MissingRequiredFields)?,
            upload_id: upload.upload_id.context(MissingRequiredFields)?,
            expected_bucket_owner: upload.owner.and_then(|o| o.id),
            request_payer: None,
        }).await.context(AbortUploadFailed)?;
        let _charged = result.request_charged.context(NotChargedButShould)?;
        Ok(())
    }

    pub async fn abort_all_multiparts(&self) -> Result<(), Error> {
        loop {
            let response = self.client.list_multipart_uploads(rusoto_s3::ListMultipartUploadsRequest {
                bucket: self.bucket.to_string(),
                ..Default::default()
            }).await.context(ListUploadsFailed)?;

            if let Some(uploads) = response.uploads {
                let result: Vec<Result<_, _>> = self.do_many_requests(
                    uploads.into_iter().map(|upload| self.abort_upload(upload))
                ).await;
                let errors: Vec<Error> = result
                    .into_iter()
                    .filter_map(|x| x.err())
                    .collect();
                if !errors.is_empty() {
                    FailedMany { errors }.fail()?
                }
            }

            if response.is_truncated != Some(true) {
                break;
            }
        }
        Ok(())
    }
}
