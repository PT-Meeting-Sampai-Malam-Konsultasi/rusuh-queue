package clientip

import (
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestIP(t *testing.T) {
	cf, err := New("CF-Connecting-IP", []string{"173.245.48.0/20"})
	if err != nil {
		t.Fatal(err)
	}
	cloudfront, err := New("CloudFront-Viewer-Address", []string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		r      *Resolver
		remote string
		header string
		value  string
		want   string
	}{
		{"trusted proxy, header used", cf, "173.245.48.10:443", "CF-Connecting-IP", "203.0.113.7", "203.0.113.7"},
		{"untrusted peer spoofs header", cf, "198.51.100.9:5555", "CF-Connecting-IP", "203.0.113.7", "198.51.100.9"},
		{"trusted proxy, empty header", cf, "173.245.48.10:443", "CF-Connecting-IP", "", "173.245.48.10"},
		{"trusted proxy, junk header", cf, "173.245.48.10:443", "CF-Connecting-IP", "not-an-ip", "173.245.48.10"},
		{"cloudfront v4 with port", cloudfront, "10.1.2.3:80", "CloudFront-Viewer-Address", "203.0.113.7:46532", "203.0.113.7"},
		{"cloudfront v6 with port, no brackets", cloudfront, "10.1.2.3:80", "CloudFront-Viewer-Address", "2001:db8::1:46532", "2001:db8::1"},
		{"bracketed v6", cloudfront, "10.1.2.3:80", "CloudFront-Viewer-Address", "[2001:db8::2]:443", "2001:db8::2"},
		{"v4-mapped peer", cf, "[::ffff:198.51.100.9]:5555", "", "", "198.51.100.9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/queue/devices", nil)
			req.RemoteAddr = tt.remote
			if tt.header != "" {
				req.Header.Set(tt.header, tt.value)
			}
			if got := tt.r.IP(req); got.String() != tt.want {
				t.Fatalf("IP = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSubnet(t *testing.T) {
	if got := Subnet(netip.MustParseAddr("203.0.113.77")).String(); got != "203.0.113.0/24" {
		t.Fatalf("v4 subnet = %s", got)
	}
	if got := Subnet(netip.MustParseAddr("2001:db8:1:2::9")).String(); got != "2001:db8:1::/48" {
		t.Fatalf("v6 subnet = %s", got)
	}
}

func TestNewRejectsBadCIDR(t *testing.T) {
	if _, err := New("CF-Connecting-IP", []string{"10.0.0.0/33"}); err == nil {
		t.Fatal("want error")
	}
}
