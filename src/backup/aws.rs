use futures::StreamExt;
use rusoto_s3::*;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use std::future::Future;
use tokio::io::AsyncRead;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Etag(pub String);

impl Etag {
    pub fn unhex(&self) -> Option<Vec<u8>> {
        hex::decode(&self.0).ok()
    }
}

pub struct Aws {
    bucket: String,
    client: S3Client,
    max_parallel: usize,
}

#[derive(Debug, Snafu)]
pub enum Error {
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
    SimpleUploadFailed {
        source: rusoto_core::RusotoError<PutObjectError>,
    },
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

    fn storage_class(&self) -> Option<String> {
        Some("DEEP_ARCHIVE".to_string())
    }

    pub async fn begin_upload(&self, name: String) -> Result<MultipartUpload<'_>, Error> {
        let upload = self
            .client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: name,
                storage_class: self.storage_class(),
                ..CreateMultipartUploadRequest::default()
            })
            .await
            .context(CreateUploadFailed)?;
        Ok(MultipartUpload {
            root: self,
            bucket: upload.bucket.context(MissingRequiredFields)?,
            id: upload.upload_id.context(MissingRequiredFields)?,
            key: upload.key.context(MissingRequiredFields)?,
            parts: Vec::new(),
        })
    }

    pub async fn upload<R: AsyncRead + Send + Sync + 'static>(
        &mut self,
        name: String,
        data: R,
        md5: Option<impl md5::Digest>,
        size: Option<i64>,
    ) -> Result<Etag, Error> {
        let res = self
            .client
            .put_object(PutObjectRequest {
                body: Some(read_to_body(data)),
                bucket: self.bucket.clone(),
                content_length: size,
                content_md5: md5.map(|x| base64::encode(x.finalize())),
                key: name,
                metadata: None,
                storage_class: self.storage_class(),
                ..Default::default()
            })
            .await
            .context(SimpleUploadFailed)?;
        let etag = res.e_tag.context(MissingRequiredFields)?;
        Ok(Etag(etag))
    }
}

fn read_to_body(reader: impl AsyncRead + Send + Sync + 'static) -> rusoto_core::ByteStream {
    let stream = tokio_util::codec::FramedRead::new(reader, tokio_util::codec::BytesCodec::new());
    let stream = stream.map(|x| x.map(|b| b.into()));
    rusoto_core::ByteStream::new(stream)
}

pub struct MultipartUpload<'a> {
    root: &'a Aws,
    bucket: String,
    id: String,
    key: String,
    parts: Vec<UploadedPart>,
}

pub struct UploadedPart {
    pub etag: Etag,
    pub number: i64,
}

impl<'a> MultipartUpload<'a> {
    pub async fn upload_part<R: AsyncRead + Send + Sync + 'static>(
        &mut self,
        part: R,
        md5: Option<impl md5::Digest>,
        size: Option<i64>,
    ) -> Result<&UploadedPart, Error> {
        let number = (self.parts.len() + 1) as i64;
        let response = self
            .root
            .client
            .upload_part(UploadPartRequest {
                body: Some(read_to_body(part)),
                bucket: self.bucket.clone(),
                content_length: size,
                content_md5: md5.map(|x| base64::encode(x.finalize())),
                key: self.key.clone(),
                part_number: number,
                upload_id: self.id.clone(),
                ..Default::default()
            })
            .await
            .context(UploadPartFailed)?;
        let part = UploadedPart {
            etag: Etag(response.e_tag.context(MissingRequiredFields)?),
            number,
        };
        self.parts.push(part);
        Ok(self.parts.last().unwrap())
    }

    pub async fn complete(self) -> Result<(), (MultipartUpload<'a>, Error)> {
        let expected = self.expected_etag();
        let res = self
            .root
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
        let found = res.e_tag.map(Etag);
        if found != expected {
            log!(warn: "Unexpected etag in multipart upload: {:?} != {:?}", expected, found);
        }
        Ok(())
    }

    pub fn expected_etag(&self) -> Option<Etag> {
        use digest::Digest;
        let mut digest = md5::Md5::new();
        for part in &self.parts {
            digest.update(&part.etag.unhex()?);
        }
        let fin = digest.finalize();
        let res = format!("{:x}-{}", fin, self.parts.len());
        Some(Etag(res))
    }
}
