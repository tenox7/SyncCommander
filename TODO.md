# TODO

## Bugs


## Features
- support remote move with relative and absolute paths, ask for path
- support resume/continue/append and intelligent upload/downloads for rsync
  especially if files are there but different sizes
  if no file present on one side then always do a full copy
- support for retry upload on stale/conn drop etc 
- support multiple concurrent remote connections and spread listing across dirs, multiple file copies
- set base directory
- open/collapse all
- select multiple files / folders and execute a batch
- export to csv/xls
- syncthing
- misc protocols
  - smb/cifs
  - nfs
  - s3/gs
  - http scraping
  - afero fs lib
