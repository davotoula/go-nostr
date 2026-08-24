//go:build !js

package nostr

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// The three data races that survive the Relay.close() capture-before-cancel fix.
//
// All three are on the same two fields — r.Connection and r.ConnectionError —
// written by the goroutines ConnectWithTLS starts and read elsewhere without
// closeMutex held. The close() fix removed the deref on one path; it did not
// make either field safe.
//
// Each of these fails under -race against the parent of the commit that fixes
// them. They are loops rather than single shots because a race detector reports
// what it observes, and one interleaving is not reliably the losing one:
// measured at raceReps=1 the three tests reported 3/1/2/0/0, 1/0/0/0/0 and
// 0/0/2/0/0 races over five runs each, and at raceReps=30 they report reliably.
const raceReps = 30

// 1. Relay.Subscribe reads r.Connection while the writer goroutine nils it.
//
// Reachable as soon as anything subscribes — which is what NWC does. Held off
// today only because the consumer has no subscriptions yet.
func TestRaceSubscribeAgainstConnectionNilling(t *testing.T) {
	ws := newWebsocketServer(discardingHandler)
	defer ws.Close()

	for range raceReps {
		r := mustRelayConnect(t, ws.URL)
		var wg sync.WaitGroup
		wg.Add(2)
		// The writer goroutine nils r.Connection when the context is done.
		go func() { defer wg.Done(); r.Close() }()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			//nolint:errcheck // the answer does not matter; the READ is the race
			r.Subscribe(ctx, Filters{{Kinds: []int{1}}})
		}()
		wg.Wait()
	}
}

// 2. close()'s own read of r.Connection races when the cancellation came from a
// PARENT context rather than from close() itself.
//
// The capture-before-cancel fix orders close()'s read before ITS OWN cancel. A
// parent cancelling concurrently starts the writer goroutine's nilling with no
// such ordering, and closeMutex does not serialise against that goroutine.
//
// NewRelay, NOT RelayConnect. RelayConnect passes context.Background() as the
// connection context and says so in its own doc — "the ongoing relay connection
// uses a background context" — so a parent cancel can never reach the writer
// goroutine through it. That IS the caller property holding this race off
// today, and NewRelay is the door a caller opens when it wants long-term
// connection contexts.
func TestRaceParentCancelAgainstClose(t *testing.T) {
	ws := newWebsocketServer(discardingHandler)
	defer ws.Close()

	for range raceReps {
		parent, cancelParent := context.WithCancel(context.Background())
		r := NewRelay(parent, ws.URL)
		ctx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
		err := r.Connect(ctx)
		cancelDial()
		if err != nil {
			cancelParent()
			t.Fatalf("connect: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancelParent() }()
		go func() { defer wg.Done(); r.Close() }()
		wg.Wait()
	}
}

// 3. r.ConnectionError is written by the reader goroutine when ReadMessage
// fails and read by the writer goroutine when the connection context is done,
// with nothing between them.
//
// The hangup alone is not enough, and that is worth knowing: on that path the
// reader writes the field and then cancels the context ITSELF, which orders the
// writer's read after it. Only an independent cancel — the Close() below —
// makes the two concurrent. A live subscription is what keeps the reader
// goroutine in ReadMessage while that cancel lands.
func TestRaceConnectionErrorWriteAgainstRead(t *testing.T) {
	ws := newWebsocketServer(func(conn *websocket.Conn) {
		time.Sleep(4 * time.Millisecond)
		conn.Close()
	})
	defer ws.Close()

	for range raceReps {
		r, err := RelayConnect(context.Background(), ws.URL)
		if err != nil {
			// The server may hang up before the handshake completes; that
			// interleaving simply has nothing to race.
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		//nolint:errcheck // the subscription keeps the reader goroutine busy
		r.Subscribe(ctx, Filters{{Kinds: []int{1}}})
		go r.Close()
		cancel()
	}
}

// The bound the fix introduces, measured rather than assumed.
//
// close() holds closeMutex across conn.Close(), which is coder/websocket's
// Close(StatusNormalClosure) and waits for the peer's close frame. The write
// side now takes that same mutex, so the writer goroutine can block for as long
// as that wait lasts. It was already true of close() itself; it is new for the
// writer goroutine, and it is the one property this patch trades for.
//
// Measured against a peer that accepts and then never answers — the worst case
// a real relay can produce.
func TestCloseAgainstASilentPeerIsBounded(t *testing.T) {
	ws := newWebsocketServer(func(conn *websocket.Conn) { select {} })
	defer ws.Close()

	r := mustRelayConnect(t, ws.URL)

	start := time.Now()
	_ = r.Close()
	closeTook := time.Since(start)
	t.Logf("close() against a silent peer took %v", closeTook)

	// The assertion is deliberately loose: the exact figure belongs to
	// coder/websocket — its close handshake uses a hard-coded five-second
	// timeout — and will move when that library is upgraded. What must stay
	// true is that the wait is BOUNDED, because an unbounded one would hold the
	// writer goroutine for the life of the process.
	if closeTook > 30*time.Second {
		t.Errorf("close() took %v against a silent peer; the writer goroutine waits on the "+
			"same mutex, so this is how long a publish can stall", closeTook)
	}
}
