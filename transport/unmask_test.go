package transport

import "testing"

// U19: the open dialog shows an argument masked, and gave the real secrets
// back only for text left as it was: any edit sent the mask as the
// password, and the login failed. Now the secrets go back into the text
// as edited, for the same place only: a secret left as the mask in an
// argument for another place is refused, never sent there, and the old
// secret never goes with it.
func TestUnmask(t *testing.T) {
	const s3 = "rclone://:s3,provider=AWS,access_key_id=AK,secret_access_key=SK:bucket/a"
	for _, c := range []struct {
		original, edited, want string // want "" for refused
	}{
		// The same place: the secrets go back into the text as edited.
		{"sftp://u:secret@host/a", "sftp://u:xxxxx@host/a", "sftp://u:secret@host/a"},
		{"sftp://u:secret@host/a", "sftp://u:xxxxx@host/c", "sftp://u:secret@host/c"},
		{"sftp://u:secret@host:22/a", "sftp://u:xxxxx@host:22", "sftp://u:secret@host:22"},
		{"sftp://u:secret@host", "sftp://u:xxxxx@host/c", "sftp://u:secret@host/c"},
		{"sftp://u:pa/ss@host/a", "sftp://u:xxxxx@host/b", "sftp://u:pa/ss@host/b"},
		{s3, "rclone://:s3,provider=AWS,access_key_id=xxxxx,secret_access_key=xxxxx:bucket/b",
			"rclone://:s3,provider=AWS,access_key_id=AK,secret_access_key=SK:bucket/b"},
		{"rclone://:http,url='https://bob:pw@host/':x", "rclone://:http,url='https://bob:xxxxx@host/':y",
			"rclone://:http,url='https://bob:pw@host/':y"},
		// Typed anew, or nothing hidden: the text as it is.
		{"sftp://u:secret@host/a", "sftp://u:new@host/c", "sftp://u:new@host/c"},
		{"sftp://u:secret@host/a", "sftp://u@other/c", "sftp://u@other/c"},
		{"sftp://u:secret@host/a", "sftp://u@xxxxx.example/c", "sftp://u@xxxxx.example/c"},
		{"sftp://u:abxxxxxcd@host/a", "sftp://u:abxxxxxcd@host", "sftp://u:abxxxxxcd@host"},
		{"sftp://u@host/a", "sftp://u@host/b", "sftp://u@host/b"},
		{"/data/a", "/data/b", "/data/b"},
		{"/data/a", "sftp://u:xxxxx@host/a", "sftp://u:xxxxx@host/a"},
		// A secret left as the mask for another place: refused.
		{"sftp://u:secret@host/a", "sftp://u:xxxxx@other/a", ""},
		{"sftp://u:secret@host/a", "sftp://v:xxxxx@host/a", ""},
		{"sftp://u:secret@host/a", "ftp://u:xxxxx@host/a", ""},
		{"sftp://u:secret@host/a", "sftp://u:xxxxx@host:2222/a", ""},
		{s3, "rclone://:s3,provider=Minio,access_key_id=xxxxx,secret_access_key=xxxxx:bucket/a", ""},
		{s3, "rclone://:s3,provider=AWS,access_key_id=NEW,secret_access_key=xxxxx,region=eu:bucket/a", ""},
		{"rclone://:http,url='https://bob:pw@host/':x", "rclone://:http,url='https://bob:xxxxx@evil/':x", ""},
	} {
		got, err := Unmask(c.original, c.edited)
		switch {
		case c.want == "" && err == nil:
			t.Errorf("%s edited to %s: gave %s; want it refused", c.original, c.edited, got)
		case c.want != "" && (err != nil || got != c.want):
			t.Errorf("%s edited to %s: gave %q, %v; want %s", c.original, c.edited, got, err, c.want)
		}
	}
}
