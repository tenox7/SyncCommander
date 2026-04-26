# TODO

## Features

- progress bar for file copy
- support resume/continue/append and intelligent upload/downloads for rsync
  especially if files are there but different sizes
  if no file present on one side then always do a full copy
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
