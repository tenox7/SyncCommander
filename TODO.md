# TODO

- in rsync module name require and module not found are fatal errors should not retry

## Features

- symlink support ?
  rclone webdav: Can't follow symlink without -L/--copy-links
  currently using --local-links - needs more testing especially with --copy-links
- select multiple files with tab or something and copy all at once
- detect renames via CRC on and rename files (no rsync)
- sort 

- concurrent uploads within one file (subparts) if protocol supports

- remote move with relative and absolute paths, ask for path
- color schemes
- misc protocols
  - rclone serve
  - rustic rest
  - syncthing protocol
  - smb/cifs
  - nfs
  - s3/gs with ETag
  - http scraping
  - afero fs lib, etc
  - rclone other protos
  - restic other protos, incl restic http server
  - archive.org
- goreleaser
