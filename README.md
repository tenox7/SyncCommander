# Sync Commander

A tool for manual comparison, inspection, verification and troubleshooting of file/directory tree synchronization. Like Norton Commander or Midnight Commander but for sync.

## Rationale

- Out of band verification and inspection of dir sync tree
- Troubleshooting, debugging sync issues
- Manual touch up and maintenance

## Supported protocols and checksums

- Local dir including remote mounts. SHA/MD5 checksums.
- ftp://  ftps://  ftpes://  with implicit/explicit TLS. XCRC, XSHA, HASH.
- sftp:// scp:// ssh://. SHA/MD5 over ssh.
- rsync://, rsync+ssh://. Rsync MD4 and SHA/MD5 over ssh.


