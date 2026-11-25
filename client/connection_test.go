package client

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

func TestWireConnSendsAndReceivesFrame(t *testing.T) {
	clientSide, brokerSide := net.Pipe()
	defer brokerSide.Close()

	c := newWireConn(clientSide)
	defer c.Close()

	brokerErr := make(chan error, 1)
	go func() {
		request, err := protocol.ReadFrame(brokerSide)
		if err != nil {
			brokerErr <- err
			return
		}
		if request.Type != protocol.TypeDeleteTopic || request.TopicID != 9 {
			brokerErr <- errUnexpectedFrame
			return
		}
		brokerErr <- protocol.WriteFrame(brokerSide, protocol.Frame{Type: protocol.TypeOK})
	}()

	if err := c.send(protocol.Frame{Type: protocol.TypeDeleteTopic, TopicID: 9}); err != nil {
		t.Fatalf("send(): %v", err)
	}

	got, err := c.receive(context.Background())
	if err != nil {
		t.Fatalf("receive(): %v", err)
	}
	if got.Type != protocol.TypeOK {
		t.Fatalf("resposta = %+v, esperado OK", got)
	}
	if err := <-brokerErr; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestWireConnSerializesConcurrentWrites(t *testing.T) {
	const total = 20
	clientSide, brokerSide := net.Pipe()
	defer brokerSide.Close()

	c := newWireConn(clientSide)
	defer c.Close()

	brokerErr := make(chan error, 1)
	go func() {
		seen := make(map[uint8]bool, total)
		for i := 0; i < total; i++ {
			frame, err := protocol.ReadFrame(brokerSide)
			if err != nil {
				brokerErr <- err
				return
			}
			if frame.Type != protocol.TypeDeleteTopic || frame.TopicID == 0 || seen[frame.TopicID] {
				brokerErr <- errUnexpectedFrame
				return
			}
			seen[frame.TopicID] = true
		}
		brokerErr <- nil
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, total)
	for i := 1; i <= total; i++ {
		wg.Add(1)
		go func(id uint8) {
			defer wg.Done()
			errCh <- c.send(protocol.Frame{Type: protocol.TypeDeleteTopic, TopicID: id})
		}(uint8(i))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("send concorrente: %v", err)
		}
	}
	if err := <-brokerErr; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestWireConnCloseUnblocksReceive(t *testing.T) {
	clientSide, brokerSide := net.Pipe()
	defer brokerSide.Close()

	c := newWireConn(clientSide)
	if err := c.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if _, err := c.receive(context.Background()); err == nil {
		t.Fatal("receive() deveria retornar erro após Close")
	}
}

var errUnexpectedFrame = testError("quadro inesperado")

type testError string

func (e testError) Error() string { return string(e) }
