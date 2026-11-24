package client

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/client/localqueue"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

func startPublisherTestServer(t *testing.T, serve func(net.Conn) error) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		defer listener.Close()
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- serve(conn)
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().String(), done
}

func readExpectedFrame(conn net.Conn, want byte) (protocol.Frame, error) {
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return protocol.Frame{}, err
	}
	if frame.Type != want {
		return protocol.Frame{}, errors.New("quadro inesperado: " + protocol.TypeName(frame.Type))
	}
	return frame, nil
}

func answer(conn net.Conn, frame protocol.Frame) error {
	return protocol.WriteFrame(conn, frame)
}

func servePublisherHandshake(conn net.Conn) error {
	hello, err := readExpectedFrame(conn, protocol.TypeHello)
	if err != nil {
		return err
	}
	if hello.Version != protocol.Version || hello.ClientID != 42 {
		return errors.New("HELLO com versão ou client_id incorreto")
	}
	return answer(conn, protocol.Frame{Type: protocol.TypeStatus, Role: protocol.RolePrincipal, Epoch: 1})
}

func waitForPendingCount(t *testing.T, p *publisher, want int) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := p.queue.Pending()
		if err != nil {
			t.Fatalf("Pending(): %v", err)
		}
		if len(entries) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, err := p.queue.Pending()
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("quantidade de pendências = %d, esperada %d", len(entries), want)
}

func TestPublisherPersistsUntilPublishAck(t *testing.T) {
	publishSeen := make(chan protocol.Frame, 1)
	allowAck := make(chan struct{})
	allowServerExit := make(chan struct{})
	address, serverDone := startPublisherTestServer(t, func(conn net.Conn) error {
		if err := servePublisherHandshake(conn); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeError, ErrCode: protocol.CodeTopicNotFound}); err != nil {
			return err
		}
		create, err := readExpectedFrame(conn, protocol.TypeCreateTopic)
		if err != nil {
			return err
		}
		if create.TopicName != "mar/temperatura" {
			return errors.New("nome incorreto em CREATE_TOPIC")
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 7}); err != nil {
			return err
		}
		publish, err := readExpectedFrame(conn, protocol.TypePublish)
		if err != nil {
			return err
		}
		publishSeen <- publish
		<-allowAck
		if err := answer(conn, protocol.Frame{Type: protocol.TypePublishAck}); err != nil {
			return err
		}
		<-allowServerExit
		return nil
	})

	queueDir := t.TempDir()
	pubInterface, err := NewPublisher(Config{
		Brokers:  []string{address},
		ClientID: 42,
		QueueDir: queueDir,
	})
	if err != nil {
		t.Fatalf("NewPublisher(): %v", err)
	}
	pub := pubInterface.(*publisher)
	payload := []byte{0, 7, 0, 0, 0, 1, 0, 0, 0, 2, 0x0A, 0x96}
	if err := pub.Publish("mar/temperatura", true, payload); err != nil {
		t.Fatalf("Publish(): %v", err)
	}

	select {
	case frame := <-publishSeen:
		if frame.TopicID != 7 || frame.Flags != protocol.FlagCritical || string(frame.Payload) != string(payload) {
			t.Fatalf("PUBLISH incorreto: %+v", frame)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("o broker falso não recebeu PUBLISH")
	}

	waitForPendingCount(t, pub, 1) // sem PUBLISH_ACK, a leitura continua na fila
	close(allowAck)
	waitForPendingCount(t, pub, 0) // só sai da fila depois do ACK
	close(allowServerExit)

	if err := pub.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker falso: %v", err)
	}

	queuePath := filepath.Join(queueDir, "publisher-42.queue")
	q, err := localqueue.Open(queuePath, publisherQueueLimit)
	if err != nil {
		t.Fatalf("reabrir fila: %v", err)
	}
	defer q.Close()
	pending, err := q.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("fila deveria estar vazia depois do ACK: %+v", pending)
	}
}

func TestPublisherReportsTerminalErrorAndRemovesEntry(t *testing.T) {
	allowServerExit := make(chan struct{})
	address, serverDone := startPublisherTestServer(t, func(conn net.Conn) error {
		if err := servePublisherHandshake(conn); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 3}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypePublish); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeError, ErrCode: protocol.CodeNoSubscribers}); err != nil {
			return err
		}
		<-allowServerExit
		return nil
	})

	reported := make(chan error, 1)
	pubInterface, err := NewPublisher(Config{
		Brokers:  []string{address},
		ClientID: 42,
		QueueDir: t.TempDir(),
		OnError:  func(_ string, _ []byte, err error) { reported <- err },
	})
	if err != nil {
		t.Fatalf("NewPublisher(): %v", err)
	}
	pub := pubInterface.(*publisher)
	if err := pub.Publish("mar/temperatura", false, []byte{1, 2, 3}); err != nil {
		t.Fatalf("Publish(): %v", err)
	}

	select {
	case err := <-reported:
		var brokerErr *brokerError
		if !errors.As(err, &brokerErr) || brokerErr.code != protocol.CodeNoSubscribers {
			t.Fatalf("OnError recebeu %v, esperado NO_SUBSCRIBERS", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("OnError não foi chamado")
	}
	waitForPendingCount(t, pub, 0)
	close(allowServerExit)
	if err := pub.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestPublisherRetriesTransientErrorWithoutLosingEntry(t *testing.T) {
	firstPublish := make(chan struct{})
	allowServerExit := make(chan struct{})
	address, serverDone := startPublisherTestServer(t, func(conn net.Conn) error {
		if err := servePublisherHandshake(conn); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 5}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypePublish); err != nil {
			return err
		}
		close(firstPublish)
		if err := answer(conn, protocol.Frame{Type: protocol.TypeError, ErrCode: protocol.CodeRecovering}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypePublish); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypePublishAck}); err != nil {
			return err
		}
		<-allowServerExit
		return nil
	})

	pub, err := NewPublisher(Config{
		Brokers: []string{address}, ClientID: 42, QueueDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewPublisher(): %v", err)
	}
	p := pub.(*publisher)
	if err := pub.Publish("mar/temperatura", false, []byte{4, 5, 6}); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case <-firstPublish:
	case <-time.After(4 * time.Second):
		t.Fatal("primeira publicação não chegou ao broker")
	}
	waitForPendingCount(t, p, 1)
	waitForPendingCount(t, p, 0)
	close(allowServerExit)
	if err := pub.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestPublisherRejectsInvalidPayload(t *testing.T) {
	pubInterface, err := NewPublisher(Config{
		Brokers: []string{"127.0.0.1:1"}, QueueDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	pub := pubInterface
	defer pub.Close()
	if err := pub.Publish("mar/temperatura", false, nil); err == nil {
		t.Fatal("payload vazio deveria ser rejeitado")
	}
	if err := pub.Publish("mar/temperatura", false, make([]byte, protocol.MaxPayloadSize+1)); err == nil {
		t.Fatal("payload acima do limite deveria ser rejeitado")
	}
}

func TestPublisherRejectsPublishAfterClose(t *testing.T) {
	pub, err := NewPublisher(Config{
		Brokers: []string{"127.0.0.1:1"}, QueueDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish("mar/temperatura", false, []byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish() após Close() = %v, esperado ErrClosed", err)
	}
}
