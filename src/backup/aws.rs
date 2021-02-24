use futures::StreamExt;
use rusoto_s3::*;
use snafu::{OptionExt, ResultExt, Snafu};
use std::future::Future;
use tokio::io::AsyncRead;

struct Aws {
    bucket: String,
    client: S3Client,
    max_parallel: usize,
}

#[derive(Debug, Snafu)]
enum Error {
    FailedMany {
        errors: Vec<Error>,
    },
    MissingRequiredFields {
        backtrace: snafu::Backtrace,
    },
    ListUploadsFailed {
        source: rusoto_core::RusotoError<ListMultipartUploadsError>,
    },
    AbortUploadFailed {
        source: rusoto_core::RusotoError<AbortMultipartUploadError>,
    },
    CreateUploadFailed {
        source: rusoto_core::RusotoError<CreateMultipartUploadError>,
    },
    UploadPartFailed {
        source: rusoto_core::RusotoError<UploadPartError>,
    },
    CompleteUploadFailed {
        source: rusoto_core::RusotoError<CompleteMultipartUploadError>,
    },
}

pub struct Finally<T, FOk, FAbort> {
    value: T,
    right: FOk,
    fail: FAbort,
}

impl Aws {
    async fn do_many_requests<I>(&self, iter: I) -> Vec<<I::Item as Future>::Output>
    where
        I: IntoIterator,
        I::Item: Future,
    {
        futures::stream::iter(iter)
            .buffer_unordered(self.max_parallel)
            .collect()
            .await
    }

    async fn abort_upload(&self, upload: rusoto_s3::MultipartUpload) -> Result<(), Error> {
        let _result = self
            .client
            .abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: self.bucket.to_string(),
                key: upload.key.context(MissingRequiredFields)?,
                upload_id: upload.upload_id.context(MissingRequiredFields)?,
                expected_bucket_owner: upload.owner.and_then(|o| o.id),
                request_payer: None,
            })
            .await
            .context(AbortUploadFailed)?;
        Ok(())
    }

    pub async fn abort_all_multiparts(&self) -> Result<(), Error> {
        loop {
            let response = self
                .client
                .list_multipart_uploads(ListMultipartUploadsRequest {
                    bucket: self.bucket.to_string(),
                    ..Default::default()
                })
                .await
                .context(ListUploadsFailed)?;

            if let Some(uploads) = response.uploads {
                let result: Vec<Result<_, _>> = self
                    .do_many_requests(uploads.into_iter().map(|upload| self.abort_upload(upload)))
                    .await;
                let errors: Vec<Error> = result.into_iter().filter_map(|x| x.err()).collect();
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

    pub async fn begin_upload(&self, name: String) -> Result<MultipartUpload<'_>, Error> {
        let upload = self
            .client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: name,
                storage_class: Some("DEEP_ARCHIVE".to_string()),
                ..CreateMultipartUploadRequest::default()
            })
            .await
            .context(CreateUploadFailed)?;
        Ok(MultipartUpload {
            root: self,
            bucket: upload.bucket.context(MissingRequiredFields)?,
            id: upload.upload_id.context(MissingRequiredFields)?,
            key: upload.key.context(MissingRequiredFields)?,
            counter: 1,
        })
    }
}

struct MultipartUpload<'a> {
    root: &'a Aws,
    bucket: String,
    id: String,
    key: String,
    counter: u16,
}

impl<'a> MultipartUpload<'a> {
    async fn upload_part<R: 'static>(&mut self, part: R) -> Result<(), Error>
    where
        R: AsyncRead + std::marker::Send + Sync,
    {
        let stream = tokio_util::codec::FramedRead::new(part, tokio_util::codec::BytesCodec::new());
        let stream = stream.map(|x| x.map(|b| b.into()));
        self.root
            .client
            .upload_part(UploadPartRequest {
                body: Some(rusoto_core::ByteStream::new(stream)),
                bucket: self.bucket.clone(),
                content_length: None,
                content_md5: None,
                key: self.key.clone(),
                part_number: self.counter as _,
                upload_id: self.id.clone(),
                ..Default::default()
            })
            .await
            .context(UploadPartFailed)?;
        self.counter += 1;
        Ok(())
    }

    async fn complete(self) -> Result<(), (MultipartUpload<'a>, Error)> {
        self.root
            .client
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.id.clone(),
                ..Default::default()
            })
            .await
            .context(CompleteUploadFailed)
            .map_err(|e| (self, e))?;
        Ok(())
    }
}
