// Package turnstile verifies Cloudflare Turnstile tokens with siteverify.
// Cloudflare's test secrets: 1x0000000000000000000000000000000AA always
// passes, 2x0000000000000000000000000000000AA always fails.
package turnstile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"
)

const DefaultVerifyURL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"

var (
	// ErrRejected means the token is missing, wrong, expired or reused.
	ErrRejected = errors.New("turnstile: token rejected")
	// ErrUnavailable means siteverify could not answer; the caller should retry.
	ErrUnavailable = errors.New("turnstile: siteverify unavailable")
)

type Client struct {
	secret    string
	verifyURL string
	hostnames []string
	http      *http.Client
}

// New builds a verifier. hostnames, when set, must contain the hostname
// Cloudflare reports for the widget, so a token minted on another site fails.
func New(secret, verifyURL string, hostnames []string) *Client {
	if verifyURL == "" {
		verifyURL = DefaultVerifyURL
	}
	return &Client{
		secret:    secret,
		verifyURL: verifyURL,
		hostnames: hostnames,
		http:      &http.Client{Timeout: 2 * time.Second},
	}
}

type siteverifyResponse struct {
	Success    bool     `json:"success"`
	ErrorCodes []string `json:"error-codes"`
	Hostname   string   `json:"hostname"`
}

// Verify checks one token for the caller's IP.
func (c *Client) Verify(ctx context.Context, token string, remoteIP netip.Addr) error {
	token = strings.TrimSpace(token)
	if token == "" || len(token) > 2048 {
		return ErrRejected
	}

	form := url.Values{"secret": {c.secret}, "response": {token}}
	if remoteIP.IsValid() {
		form.Set("remoteip", remoteIP.String())
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.verifyURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}

	var out siteverifyResponse
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 64<<10)).Decode(&out); err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if !out.Success {
		if slices.Contains(out.ErrorCodes, "internal-error") {
			return fmt.Errorf("%w: %v", ErrUnavailable, out.ErrorCodes)
		}
		return fmt.Errorf("%w: %v", ErrRejected, out.ErrorCodes)
	}
	if len(c.hostnames) > 0 && !slices.Contains(c.hostnames, out.Hostname) {
		return fmt.Errorf("%w: hostname %q", ErrRejected, out.Hostname)
	}
	return nil
}
