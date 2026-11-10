package token

import "github.com/golang-jwt/jwt/v5"

// Claims is implemented by the three token payloads below.
type Claims interface {
	jwt.Claims
	registered() *jwt.RegisteredClaims
}

// DeviceClaims: `sub` is the device id.
type DeviceClaims struct {
	Platform string `json:"plat"`
	jwt.RegisteredClaims
}

// QueueClaims: `sub` is the device id; the place is (evt, seq).
type QueueClaims struct {
	Event string `json:"evt"`
	Seq   int64  `json:"seq"`
	jwt.RegisteredClaims
}

// AdmissionClaims: `sub` is the device id; exp is the end of the slot's lease
// (the 5-minute order window), so core needs no lease lookup (tokens.md).
type AdmissionClaims struct {
	Event    string `json:"evt"`
	Seq      int64  `json:"seq"`
	Slot     string `json:"slot"`
	Reserved int    `json:"resv"`
	jwt.RegisteredClaims
}

func (c *DeviceClaims) registered() *jwt.RegisteredClaims    { return &c.RegisteredClaims }
func (c *QueueClaims) registered() *jwt.RegisteredClaims     { return &c.RegisteredClaims }
func (c *AdmissionClaims) registered() *jwt.RegisteredClaims { return &c.RegisteredClaims }
