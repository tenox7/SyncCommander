//go:build !rclone_all

package transport

// Default rclone backend set. The linker keeps only what is imported, so this
// list is what decides the binary size: s3 alone costs ~13MB (AWS SDK) but
// covers AWS, MinIO, Wasabi, R2, Ceph, DigitalOcean and every other
// S3-compatible store, while b2/swift/crypt/http are nearly free. drive,
// azureblob and googlecloudstorage cost 7-10MB each and live behind the tag.
// Build with -tags rclone_all for all 68 backends.
import (
	_ "github.com/rclone/rclone/backend/b2"
	_ "github.com/rclone/rclone/backend/crypt"
	_ "github.com/rclone/rclone/backend/dropbox"
	_ "github.com/rclone/rclone/backend/http"
	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/onedrive"
	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/backend/smb"
	_ "github.com/rclone/rclone/backend/swift"
)
