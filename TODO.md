\# TODO

## Features

- refactor go files, there are too many/ momove some to a sub dir
- support remote move with relative and absolute paths, ask for path
- support resume/continue/append and intelligent upload/downloads for rsync
  especially if files are there but different sizes
  if no file present on one side then always do a full copy
- support for retry upload on stale/conn drop etc 
- support multiple concurrent remote connections and spread listing across dirs, multiple file copies
- open/collapse all
- select multiple files / folders and execute a batch
- open object, diff for text files, hex dump diff for binary
- export to csv/xls
- color schemes
- misc protocols
  - syncthing protocol
  - smb/cifs
  - nfs
  - s3/gs with ETag
  - http scraping
  - afero fs lib
