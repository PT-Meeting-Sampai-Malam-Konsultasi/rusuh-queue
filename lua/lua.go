// Package lua embeds the Valkey function library (rusuh_q.lua) so the binary
// carries the exact scripts it was tested with.
package lua

import _ "embed"

//go:embed rusuh_q.lua
var Library string
