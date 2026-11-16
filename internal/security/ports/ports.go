// Package ports declares what security offers other modules.
package ports

import "google.golang.org/grpc/credentials"

// PeerCredentials returns the transport credentials used to dial one peer.
// In development mode implementations return plaintext credentials.
type PeerCredentials interface {
	PeerCredentials(peerID string) credentials.TransportCredentials
}
