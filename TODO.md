# TODO

## Features

- intelligent upload/downloads for rsync (use --append-verify or checksum)
  for the case where files are there but different sizes
  (basic resume-by-append already wired for local/sftp/scp/ftp/rsync+ssh;
  rsync daemon uses delta-sync at protocol level via --no-whole-file)
- gokrazy/rsync parses but does NOT honor --inplace and --partial; it always
  goes through renameio (shadow file in dst dir + atomic rename). Patching
  the lib fork to honor --inplace would let resume avoid rewriting the
  full file on disk during delta-sync.
- rsync ↔ non-local (sftp/ftp/scp): still uses tmp dir because rsync needs
  a filesystem path and the non-local side has none. Could be avoided with
  a FIFO bridge but rsync's renameio doesn't work on FIFOs.
- support for retry upload on stale/conn drop etc, with retry counter
- support multiple concurrent remote connections (configurable)
  - use to run cocurrent dir listings in differerent subdirectories
  - concurrent file uploads
  - concurrent uploads within one file (subparts) if protocol supports
- select multiple files / folders and copy a batch
- multi file progress bar

- support remote move with relative and absolute paths, ask for path
- export to csv/xls
- color schemes
- misc protocols
  - syncthing protocol
  - smb/cifs
  - nfs
  - s3/gs with ETag
  - http scraping
  - afero fs lib
- goreleaser
