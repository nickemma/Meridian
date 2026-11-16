package adapters

import (
	"io"
	"net"
	"sync"
)

// relay is an experiment-only TCP link. Blocking closes established streams and
// rejects new ones in both directions. Production nodes do not expose a fault API.
type relay struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	blocked  bool
	closed   bool
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
}

func newRelay(target string) (*relay, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &relay{listener: listener, target: target, conns: map[net.Conn]bool{}}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			source, err := listener.Accept()
			if err != nil {
				return
			}
			r.wg.Add(1)
			go r.copy(source)
		}
	}()
	return r, nil
}
func (r *relay) copy(source net.Conn) {
	defer r.wg.Done()
	defer source.Close()
	r.mu.Lock()
	if r.blocked || r.closed {
		r.mu.Unlock()
		return
	}
	r.conns[source] = true
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.conns, source); r.mu.Unlock() }()
	destination, err := net.DialTimeout("tcp", r.target, 300000000)
	if err != nil {
		return
	}
	defer destination.Close()
	r.mu.Lock()
	if r.blocked || r.closed {
		r.mu.Unlock()
		return
	}
	r.conns[destination] = true
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.conns, destination); r.mu.Unlock() }()
	done := make(chan struct{})
	go func() { _, _ = io.Copy(destination, source); _ = destination.Close(); close(done) }()
	_, _ = io.Copy(source, destination)
	_ = source.Close()
	<-done
}
func (r *relay) block(value bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocked = value
	if value {
		for conn := range r.conns {
			_ = conn.Close()
		}
	}
}
func (r *relay) close() {
	r.mu.Lock()
	r.closed = true
	for conn := range r.conns {
		_ = conn.Close()
	}
	r.mu.Unlock()
	_ = r.listener.Close()
	r.wg.Wait()
}
