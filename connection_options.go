//go:build !js

package nostr

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/textproto"
	"syscall"

	ws "github.com/coder/websocket"
)

var defaultConnectionOptions = &ws.DialOptions{
	CompressionMode: ws.CompressionContextTakeover,
	HTTPHeader: http.Header{
		textproto.CanonicalMIMEHeaderKey("User-Agent"): {"github.com/nbd-wtf/go-nostr"},
	},
}

func getConnectionOptions(relayURL string, requestHeader http.Header, tlsConfig *tls.Config,
	checkAddr DialAddressCheck) *ws.DialOptions {
	if requestHeader == nil && tlsConfig == nil && checkAddr == nil {
		return defaultConnectionOptions
	}

	// Keep-alives off. A websocket hijacks its connection, so pooling never
	// helps one; it only ever applies to a handshake the relay REFUSED, where
	// net/http drains the small body and returns the socket to this
	// throwaway transport's idle pool. Nothing closes that pool and it has no
	// IdleConnTimeout, so the socket and its read goroutine would be held until
	// the peer gave up. That matters more now than it did: before this commit
	// the header-free, TLS-config-free case took defaultConnectionOptions and
	// http.DefaultClient, whose transport is shared and does time idle
	// connections out. A caller that sets a dial check takes this path for
	// EVERY relay — including ones a stranger named.
	transport := &http.Transport{TLSClientConfig: tlsConfig, DisableKeepAlives: true}
	if checkAddr != nil {
		// Control runs after the name is resolved and before the socket is
		// connected, and refusing there still prevents the connection. It is
		// the only pre-connect hook net offers, and it receives the resolved
		// address and nothing else — so the relay's URL is closed over here.
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Control: func(network, resolved string, _ syscall.RawConn) error {
					return checkAddr(network, relayURL, resolved)
				},
			}
			return dialer.DialContext(ctx, network, addr)
		}
	}

	// A caller that passed no headers still gets the library's User-Agent, as
	// it would have from defaultConnectionOptions. Leaving this out made a
	// dial check silently change how every relay sees this client.
	if requestHeader == nil {
		requestHeader = defaultConnectionOptions.HTTPHeader
	}

	return &ws.DialOptions{
		HTTPHeader:      requestHeader,
		CompressionMode: ws.CompressionContextTakeover,
		HTTPClient:      &http.Client{Transport: transport},
	}
}
