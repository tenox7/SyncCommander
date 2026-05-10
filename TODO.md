# TODO

## Features

- select multiple files with tab or something and copy all at once
- detect renames via CRC on and rename files (no rsync)
- remote log stop scrolling if not on top
- sort files in folders and folders!
- search with /

- rsync ↔ non-local (sftp/ftp/scp): still uses tmp dir because rsync needs
  a filesystem path and the non-local side has none. Could be avoided with
  a FIFO bridge but rsync's renameio doesn't work on FIFOs.
- support multiple concurrent remote connections (configurable)
  - use to run cocurrent dir listings in differerent subdirectories
  - concurrent file uploads
  - concurrent uploads within one file (subparts) if protocol supports
- select multiple files / folders and copy a batch

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
  - rclone protos
- goreleaser
