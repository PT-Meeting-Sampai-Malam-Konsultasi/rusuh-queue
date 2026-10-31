package http

// StreamHandler serves streamQueue (ADR-0015). It has no usecase yet: the SSE
// hub, limits and drain come in M5 (IMPLEMENTATION_PLAN.md).
type StreamHandler struct{}

func NewStream() *StreamHandler {
	return &StreamHandler{}
}
