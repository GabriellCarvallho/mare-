package client

import (
	"context"
	"net"
	"sync"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// wireConn mantém uma única goroutine de leitura e uma de escrita por conexão.
// Assim, protocol.WriteFrame nunca é chamada simultaneamente na mesma conexão.
type wireConn struct {
	conn     net.Conn
	incoming chan protocol.Frame
	outgoing chan writeRequest
	done     chan struct{}

	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
}

type writeRequest struct {
	frame protocol.Frame
	done  chan error
}

func newWireConn(conn net.Conn) *wireConn {
	c := &wireConn{
		conn:     conn,
		incoming: make(chan protocol.Frame, 16),
		outgoing: make(chan writeRequest),
		done:     make(chan struct{}),
	}
	go c.readLoop()
	go c.writeLoop()
	return c
}

// send envia um quadro pela única goroutine escritora e espera o resultado.
func (c *wireConn) send(frame protocol.Frame) error {
	req := writeRequest{frame: frame, done: make(chan error, 1)}
	select {
	case c.outgoing <- req:
	case <-c.done:
		return c.connectionError()
	}

	select {
	case err := <-req.done:
		return err
	case <-c.done:
		select {
		case err := <-req.done:
			return err
		default:
			return c.connectionError()
		}
	}
}

// receive aguarda o próximo quadro recebido ou o encerramento da conexão.
func (c *wireConn) receive(ctx context.Context) (protocol.Frame, error) {
	select {
	case <-c.done:
		return protocol.Frame{}, c.connectionError()
	default:
	}

	select {
	case frame := <-c.incoming:
		return frame, nil
	case <-c.done:
		return protocol.Frame{}, c.connectionError()
	case <-ctx.Done():
		return protocol.Frame{}, ctx.Err()
	}
}

func (c *wireConn) readLoop() {
	for {
		frame, err := protocol.ReadFrame(c.conn)
		if err != nil {
			c.stop(err)
			return
		}

		select {
		case c.incoming <- frame:
		case <-c.done:
			return
		}
	}
}

func (c *wireConn) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case req := <-c.outgoing:
			err := protocol.WriteFrame(c.conn, req.frame)
			req.done <- err
			if err != nil {
				c.stop(err)
				return
			}
		}
	}
}

func (c *wireConn) stop(err error) {
	c.closeOnce.Do(func() {
		if err == nil {
			err = net.ErrClosed
		}
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		close(c.done)
		_ = c.conn.Close()
	})
}

func (c *wireConn) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.errMu.Lock()
		c.err = net.ErrClosed
		c.errMu.Unlock()
		close(c.done)
		closeErr = c.conn.Close()
	})
	return closeErr
}

func (c *wireConn) connectionError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.err != nil {
		return c.err
	}
	return net.ErrClosed
}
