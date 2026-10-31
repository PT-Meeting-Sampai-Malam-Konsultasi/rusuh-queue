// Package clientip finds the caller's IP. A proxy header is trusted only when
// the direct peer is a trusted proxy: Cloudflare in the lab (CF-Connecting-IP),
// CloudFront on AWS (CloudFront-Viewer-Address, "ip:port").
package clientip

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

type Resolver struct {
	header  string
	trusted []netip.Prefix
}

// New takes the header to read and the CIDRs allowed to set it. With no
// header or no CIDRs, only the TCP peer address is used.
func New(header string, trustedCIDRs []string) (*Resolver, error) {
	r := &Resolver{header: http.CanonicalHeaderKey(strings.TrimSpace(header))}
	for _, c := range trustedCIDRs {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		p, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, fmt.Errorf("clientip: trusted proxy %q: %w", c, err)
		}
		r.trusted = append(r.trusted, p.Masked())
	}
	return r, nil
}

// IP returns the client address for rate limits and Turnstile's remoteip.
func (r *Resolver) IP(req *http.Request) netip.Addr {
	peer := peerAddr(req.RemoteAddr)
	if r.header == "" || !r.isTrusted(peer) {
		return peer
	}
	if ip, ok := parseHeader(req.Header.Get(r.header)); ok {
		return ip
	}
	return peer
}

// Subnet groups addresses for the coarser limit: /24 for IPv4, /48 for IPv6.
func Subnet(ip netip.Addr) netip.Prefix {
	bits := 48
	if ip.Is4() {
		bits = 24
	}
	p, _ := ip.Prefix(bits)
	return p
}

func (r *Resolver) isTrusted(ip netip.Addr) bool {
	for _, p := range r.trusted {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

func peerAddr(remote string) netip.Addr {
	if ap, err := netip.ParseAddrPort(remote); err == nil {
		return ap.Addr().Unmap()
	}
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip, _ := netip.ParseAddr(host)
	return ip.Unmap()
}

// parseHeader accepts "ip", "ip:port", "[v6]:port" and CloudFront's unbracketed "v6:port".
func parseHeader(v string) (netip.Addr, bool) {
	v = strings.TrimSpace(strings.Split(v, ",")[0])
	if v == "" {
		return netip.Addr{}, false
	}
	if ip, err := netip.ParseAddr(v); err == nil {
		return ip.Unmap(), true
	}
	if ap, err := netip.ParseAddrPort(v); err == nil {
		return ap.Addr().Unmap(), true
	}
	if i := strings.LastIndexByte(v, ':'); i > 0 {
		if ip, err := netip.ParseAddr(v[:i]); err == nil {
			return ip.Unmap(), true
		}
	}
	return netip.Addr{}, false
}
