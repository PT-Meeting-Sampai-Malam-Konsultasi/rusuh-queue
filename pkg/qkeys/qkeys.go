// Package qkeys names every Valkey key the queue reads or writes
// (IMPLEMENTATION_PLAN.md "Valkey keys"). One sale event = one hash tag {evt},
// so a Lua function can touch all of an event's keys atomically in cluster mode.
package qkeys

import (
	"fmt"
	"regexp"
)

// eventID keeps ids safe inside a hash tag and a key: no braces, colons or spaces.
var eventID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidEvent reports whether id can be used in key names.
func ValidEvent(id string) bool { return eventID.MatchString(id) }

// Global keys (each on its own slot; never used inside an event's script).
const (
	// LiveEvents: events this queue has set up (q:events).
	LiveEvents = "q:events"
	// CoreEvents: events core has published for sale (core writes, queue reads; D-2).
	CoreEvents = "core:events"
	// CoreEventsChanged: core's nudge when an event is added to or removed from CoreEvents.
	CoreEventsChanged = "core:events:changed"
)

// Event holds one event's key names.
type Event struct {
	ID string

	Cfg        string // hash: state, maxSlots, rateCap, streamFrom, opensAt, endsAt
	Seq        string // last issued sequence number
	Served     string // highest admitted sequence number
	Devs       string // hash: device -> seq | "pre" | "x:<seq>"
	Accts      string // hash: user -> device
	Pre        string // set: devices waiting for the lottery
	Active     string // zset: slot -> lease end (ms)
	Leases     string // hash: slot -> lease
	Unreserved string // units free to reserve
	Resv       string // units reserved by unheld leases
	Held       string // units held by core (booking.held, M4)
	Hist       string // zset: score served, member ts
	Inv        string // zset: invitations per tick (D-7)
	Claimed    string // zset: slot scored by seq, claims of recent invitations (D-7)
	Frontier   string // the JSON served by getFrontier
	Leader     string
	Fence      string
	Done       string // hash: processed stream event ids
	Tick       string // sharded pub/sub channel: frontier every tick

	CoreStock   string // core:{evt}:stock, JSON (D-1)
	CoreSale    string // core:{evt}:sale, JSON (D-2)
	CoreChanged string // core:{evt}:changed, sharded pub/sub nudge
}

// For returns the key names of one event; id must pass ValidEvent.
func For(id string) Event {
	q := func(name string) string { return fmt.Sprintf("q:{%s}:%s", id, name) }
	c := func(name string) string { return fmt.Sprintf("core:{%s}:%s", id, name) }
	return Event{
		ID:          id,
		Cfg:         q("cfg"),
		Seq:         q("seq"),
		Served:      q("served"),
		Devs:        q("devs"),
		Accts:       q("accts"),
		Pre:         q("pre"),
		Active:      q("active"),
		Leases:      q("leases"),
		Unreserved:  q("unreserved"),
		Resv:        q("resv"),
		Held:        q("held"),
		Hist:        q("hist"),
		Inv:         q("inv"),
		Claimed:     q("claimed"),
		Frontier:    q("frontier"),
		Leader:      q("leader"),
		Fence:       q("fence"),
		Done:        q("done"),
		Tick:        q("tick"),
		CoreStock:   c("stock"),
		CoreSale:    c("sale"),
		CoreChanged: c("changed"),
	}
}
