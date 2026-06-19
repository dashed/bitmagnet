package pathsearch

import "testing"

func TestParseTarget(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"bitmagnet-pathsearch.bitmagnet.svc:50053", "bitmagnet-pathsearch.bitmagnet.svc:50053", false},
		{"127.0.0.1:50053", "127.0.0.1:50053", false},
		{"unix:/run/bitmagnet/pathsearch.sock", "unix:/run/bitmagnet/pathsearch.sock", false},
		{"unix:///run/bitmagnet/pathsearch.sock", "unix:///run/bitmagnet/pathsearch.sock", false},
		{"/run/bitmagnet/pathsearch.sock", "unix:///run/bitmagnet/pathsearch.sock", false},
		{"  127.0.0.1:50053  ", "127.0.0.1:50053", false},
		{"", "", true},
		{"   ", "", true},
	} {
		got, err := parseTarget(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseTarget(%q): expected error", tc.in)
			}

			continue
		}

		if err != nil {
			t.Errorf("parseTarget(%q): unexpected error %v", tc.in, err)

			continue
		}

		if got != tc.want {
			t.Errorf("parseTarget(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
