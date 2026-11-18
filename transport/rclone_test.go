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
		// L24: the values a list of secret names did not name went to
		// the title bar and the log. Only the options that say where the
		// remote is are shown now, and of a URL among them, no password.
		{"rclone://:http,url='https://bob:pw@host/':", "rclone://:http,url='https://bob:xxxxx@host/':"},
		{
			"rclone://:azureblob,account=acct,sas_url='https://acct.blob.core.windows.net/c?sv=1&sig=S':",
			"rclone://:azureblob,account=xxxxx,sas_url=xxxxx:",
		},
		{"rclone://:storj,access_grant=G:bucket", "rclone://:storj,access_grant=xxxxx:bucket"},
		{"rclone://:http,url='https://h/',headers='Authorization,Bearer T':", "rclone://:http,url='https://h/',headers=xxxxx:"},
		{
			"rclone://:s3,Provider=Minio,ENDPOINT='https://u:p@minio:9000',env_auth:bucket",
			"rclone://:s3,Provider=Minio,ENDPOINT='https://u:xxxxx@minio:9000',env_auth:bucket",
		},
		{"rclone://remote,shared_with_me=true:dir", "rclone://remote,shared_with_me=xxxxx:dir"},
	}
	for _, c := range cases {
		if got := MaskURLPassword(c.in); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}
