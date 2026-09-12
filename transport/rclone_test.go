package transport

import "testing"

func TestParseRcloneSpec(t *testing.T) {
	cases := []struct{ in, want string }{
		{"rclone://gdrive", "gdrive:"},
		{"rclone://gdrive/Photos/2024", "gdrive:Photos/2024"},
		{"rclone://gdrive:Photos/2024", "gdrive:Photos/2024"},
		{"rclone://s3:bucket/key", "s3:bucket/key"},
		{"rclone://:s3,provider=AWS:bucket/key", ":s3,provider=AWS:bucket/key"},
		{"rclone://gdrive,shared_with_me:file", "gdrive,shared_with_me:file"},
		{"rclone:///tmp/dir", "/tmp/dir"},
		{"rclone://x,pass='a:b'/p", "x,pass='a:b':p"},
	}
	for _, c := range cases {
		got, err := parseRcloneSpec(c.in)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.in, got, c.want)
		}
	}
	if _, err := parseRcloneSpec("rclone://"); err == nil {
		t.Error("empty remote should error")
	}
}

func TestMaskRcloneSecrets(t *testing.T) {
	cases := []struct{ in, want string }{
		{"rclone://gdrive/Photos", "rclone://gdrive/Photos"},
		{
			"rclone://:s3,provider=AWS,access_key_id=AK,secret_access_key=SK:bucket/key",
			"rclone://:s3,provider=AWS,access_key_id=xxxxx,secret_access_key=xxxxx:bucket/key",
		},
		{
			"rclone://:sftp,host=h,user=u,pass=p:/srv",
			"rclone://:sftp,host=h,user=u,pass=xxxxx:/srv",
		},
		{
			"rclone://:webdav,url='https://a:1/dav',bearer_token=T:/x",
			"rclone://:webdav,url='https://a:1/dav',bearer_token=xxxxx:/x",
		},
	}
	for _, c := range cases {
		if got := MaskURLPassword(c.in); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}
