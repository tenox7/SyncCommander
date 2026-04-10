# Sync Commander

Directory tree comparisation and synchronization tool.

Manual inspection, comparison, verification and troubleshooting of directory tree sync.

Like Norton Commander or Midnight Commander but for sync.

## Supported protocols

- Local dir including remote mounts
- ftp://  ftps://  ftpes://  with implicit/explicit TLS
- sftp:// scp:// ssh://
- rsync://, rsync+ssh://

## Remote checksum calculation

- SFTP/SCP - sha/md5 tools via SSH
- FTP - via XCRC, XSHA, HASH
- rsync - via rsync own md4
- rsync+ssh - sha/md5 tools via SSH

