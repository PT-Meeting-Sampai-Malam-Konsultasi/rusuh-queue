package token

import (
	"encoding/base64"
	"sort"
)

// JWK is one public key in a JSON Web Key Set (RFC 8037 for Ed25519).
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
}

// JWKS is the document served at /.well-known/jwks.json.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWKS lists every verifying key, signing key first.
func (k *Keyring) JWKS() JWKS {
	order := make(map[string]int, len(k.keys))
	for i, key := range k.keys {
		order[key.ID] = i
	}
	set := JWKS{Keys: make([]JWK, 0, len(k.byID))}
	for id, pub := range k.byID {
		set.Keys = append(set.Keys, JWK{
			Kty: "OKP",
			Crv: "Ed25519",
			X:   base64.RawURLEncoding.EncodeToString(pub),
			Kid: id,
			Alg: "EdDSA",
			Use: "sig",
		})
	}
	sort.Slice(set.Keys, func(i, j int) bool { return order[set.Keys[i].Kid] < order[set.Keys[j].Kid] })
	return set
}
