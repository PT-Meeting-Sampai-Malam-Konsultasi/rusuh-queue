// Package problem writes RFC 9457 problem details with the stable ErrorCode
// values from api/openapi.yaml, so web and mobile can translate them (ADR-0014).
package problem

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
)

// CodeNotImplemented marks endpoints that have no logic yet (stubs until their milestone).
// It is not in the API contract and must be gone before 13 Nov.
const CodeNotImplemented api.ErrorCode = "NOT_IMPLEMENTED"

const typeBase = "https://api.rusuh.id/problems/"

// Write sends a problem+json response.
func Write(w http.ResponseWriter, status int, code api.ErrorCode, title, detail string) {
	p := api.Problem{
		Type:   typeBase + string(code),
		Title:  title,
		Status: status,
		Code:   code,
	}
	if detail != "" {
		p.Detail = &detail
	}
	w.Header().Set("Content-Type", "application/problem+json")
	// Errors are not cached unless the handler chose otherwise (the frontier's
	// 404 is cached 1 s like the frontier itself, so a miss storm stays at the edge).
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(p)
}

// RetryAfter sends a problem with a Retry-After header, for FULL, STREAM_FULL and RATE_LIMITED.
func RetryAfter(w http.ResponseWriter, status int, code api.ErrorCode, title string, seconds int) {
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	Write(w, status, code, title, "")
}

// NotImplemented answers 501 for an endpoint stub.
func NotImplemented(w http.ResponseWriter, operationID string) {
	Write(w, http.StatusNotImplemented, CodeNotImplemented, "Not implemented", operationID+" is not implemented yet")
}
