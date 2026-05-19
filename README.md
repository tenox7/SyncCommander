# Sync Commander

A tool for manual comparison, inspection, verification and troubleshooting of file/directory tree synchronization. Like Norton Commander or Midnight Commander but for sync.

## Rationale

- Out of band verification and inspection of dir sync tree.
- Manual comparison, touch up and maintenance.
- Troubleshooting, debugging sync issues.
- Ad hoc downloads/uploads.

## Features

- Manual RSYNC by hand.
- Remote checksum calculation via variety of protocols.
- Parallel copies.
- Batch copies for small files.

## Supported protocols and checksums

- Local dir including remote mounts. SHA/MD5 checksums.
- ftp://  ftps://  ftpes://  with implicit/explicit TLS. XCRC, XSHA, HASH.
- sftp:// scp:// ssh://. SHA/MD5 over ssh.
- rsync://, rsync+ssh://. Rsync MD4 and SHA/MD5 over ssh.


