package transport

import "testing"

// F26: a password with a / in it ended the authority where the mask looked
// for the @: it found none, and the URL went to the title bar and the log
// with the password in it. Now such a password is masked too. A port and
// an @ further on in the path, as in a Nextcloud folder named for its
// user, are still shown as they are.
func TestMaskURLPassword(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"sftp://u:secret@host/a", "sftp://u:xxxxx@host/a"},
		{"sftp://u:pa/ss@host/path", "sftp://u:xxxxx@host/path"},
		{"ftp://u:/pass@host:2121/x", "ftp://u:xxxxx@host:2121/x"},
		{"sftp://u:p@ss@host/a", "sftp://u:xxxxx@host/a"},
		{"sftp://u:p:w@host/a", "sftp://u:xxxxx@host/a"},
		{"sftp://u:pw@[::1]:22/x", "sftp://u:xxxxx@[::1]:22/x"},
		{"webdavs://john:pw@cloud.example.com/dav/files/john@example.com/", "webdavs://john:xxxxx@cloud.example.com/dav/files/john@example.com/"},
		{"webdavs://cloud.example.com:8443/dav/files/john@example.com/", "webdavs://cloud.example.com:8443/dav/files/john@example.com/"},
		{"sftp://host:22/dir/a@b", "sftp://host:22/dir/a@b"},
		{"sftp://[::1]:22/a@b", "sftp://[::1]:22/a@b"},
		{"ftp://anonymous@host/x", "ftp://anonymous@host/x"},
		{"sftp://host/a@b", "sftp://host/a@b"},
		{"/local/u:p@x", "/local/u:p@x"},
	} {
		if got := MaskURLPassword(c.in); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}
