package nostr

import (
	"crypto/tls"
	"net/http"

	ws "github.com/coder/websocket"
)

var emptyOptions = ws.DialOptions{}

func getConnectionOptions(_ string, _ http.Header, _ *tls.Config, _ DialAddressCheck) *ws.DialOptions {
	// on javascript we ignore everything because there is nothing else we can do.
	// That INCLUDES DialAddressCheck, which its own doc records: there is no
	// dialer to hook here, so the check cannot be enforced on this target.
	return &emptyOptions
}
