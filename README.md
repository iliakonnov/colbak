# colbak
Cost-effective backup tool for cold storages.

# Alternatives
Here a small list of projects that you may like much more

- [mtglacier](https://github.com/vsespb/mt-aws-glacier) — written in Perl, many features, very stable
- [iceshelf](https://github.com/mrworf/iceshelf) — in Python, based on many well-established tools, with encryption and parity correction included
- [restic](https://github.com/restic/restic/issues/3202) — in Go, cold storages are not supported yet

# Disclaimer
This tool is made for my personal usage and provided as-is. I'm not responsible for any unexpected costs and data losses. Be careful.

This tool is not intended to be used with large files. Every time it changes it will be reuploaded completely.

Empty directories are not preserved, this tool is not made for exact full system backup that can be restored without additional actions.

There are unstable features and bits of unsafe here and there. 

# Features
(implemented items are checked)

1. Cost
   - [ ] Cost estimator
   - [ ] Never uploads same file twice
   - [ ] Never deletes file during retention period
   - [ ] Minimum archive size — small files will be grouped together
   - [ ] Maximum archive size — big files will be skipped with a warning
   - [ ] Ability to limit total number of requests made
   - [ ] Option to restore only chosen files
2. Performance
   - [ ] Low memory usage (ready to work with only 128MB of free memory)
   - [ ] Reduces load on HDD by reading files no more than once
   - [ ] Multithreaded upload/download (multiple files at once)
   - [x] Advanced rename detection
3. Restoring
   - [ ] Files can be restored using small bash script only
   - [ ] Using standard archive format that can be unpacked with usual tools
   - [ ] All available file metadata is preserved
   - [ ] Supports all possible filenames without any loss (especially *NIX)
   - [ ] Most of file metadata is also stored as amz-meta
   - [ ] Fancy TUI for browsing current archives
4. Reliability
   - [ ] Fully documented and tested
   - [ ] Compiled and tested weekly with the latest Rust nightly
   - [ ] Plain text machine-readable append-only log files
   - [ ] Local **sqlite** database that is almost never gets corrupted
   - [ ] Corrupted database can be re-created from log file
   - [ ] Ability to restore database from Glacier metadata
   - [ ] ETag and custom hash validation when downloading
   - [ ] ETag (md5) validation while uploading