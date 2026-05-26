# TODO

in rsync module name require and module not found are fatal errors should not retry

## Tech debt

- UI reads the live tree (refreshTree: FlattenTree/walkStats, panel View) unsynchronized while scanner goroutines mutate it. Pointer-snapshot locals stop the crash but `go test -race` still flags the loads; a real fix needs a shared lock or a UI snapshot that fits the 1M-object scale.

## Features

- symlink support
- select multiple files with tab or something and copy all at once
- detect renames via CRC on and rename files (no rsync)
- sort 

- concurrent uploads within one file (subparts) if protocol supports

- bandwidth limit across all protocols incl local fs
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
