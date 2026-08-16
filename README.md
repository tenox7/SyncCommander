# Sync Commander

A tool for manual comparison, inspection, verification and troubleshooting of file/directory tree synchronization. Like Norton Commander or Midnight Commander but for sync.

![SyncCommander](sc.png)

- Out of band verification and inspection of dir sync tree.
- Manual comparison, touch up and maintenance.
- Troubleshooting, debugging sync issues.
- Ad hoc downloads/uploads. Touch up.
- Remote checksum generation and comparison.

## Features

- Manual Rsync / Rclone by hand.
- Remote checksum calculation via variety of protocols.
- Parallel copies.
- Batch copies for small files.

## Supported protocols and checksums

| Protocol | Checksum |
| --- | --- |
| Local dir including remote mounts | XXH3, SHA256, MD5 |
| ftp:// ftps:// ftpes:// with implicit/explicit TLS | XCRC, XSHA, HASH |
| sftp:// scp:// ssh:// | SHA/MD5 (over ssh) |
| rsync://, rsync+ssh:// | Rsync MD4 (internal), SHA/MD5 (over ssh) |
| webdav://, webdavs:// | MD5/SHA1 (`rclone --etag-hash`) |
| restic://, restics:// | SHA256 |
| fake:// synthetic tree (testing) | XXH3, SHA256, SHA1, MD5, MD4 |

## Server examples

- `rclone serve webdav --etag-hash md5`
- `rclone serve restic`

## Synthetic trees for perf testing

`fake://` generates a directory tree in memory — nothing on disk, nothing on the
wire. Every attribute is derived from a hash of the path, so a 1M-object tree
costs no memory until it is listed, and two `fake://` sides with the same
parameters compare as identical.

```
sc fake://huge fake://huge                      # ~1M objects per side
sc fake://large 'fake://large?diff=0.1&drop=0.02'  # 10% differing, 2% missing right
sc 'fake://x?dirs=8&files=25&depth=5&latency=20ms' /tmp/x   # simulated network RTT
sc -pprof localhost:6060 fake://huge fake://huge           # then: go tool pprof
```

Presets (host): `tiny` ~140, `small` ~3k, `medium` ~48k, `large` ~240k,
`huge` ~1M, `insane` ~4.5M objects. Parameters override the preset:
`dirs`, `files`, `depth`, `seed`, `size`, `vary`, `drop`, `dropdirs`, `diff`,
`latency`, `cklatency`, `nodata`.

Writes land in an in-memory overlay, so `fake://` also works as a copy target.

Timing harness: `SC_PERF=fake://huge go test ./model -run TestPerfScan -v`
