# Sync Commander

Directory tree comparisation and synchronization TUI app.


## Supported protocols

- Local dir including remote mounts
- ftp://  ftps://  ftpes://  with implicit/explicit TLS
- sftp:// scp:// ssh://
- rsync://, rsync+ssh://

## Remote checksum calculation

- SFTP/SCP - sha/md5 tools via SSH
- FTP - via XCRC, XSHA, HASH
- rsync - internal md4, not exposed
- rsync+ssh - sha/md5 tools via SSH

