package client

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

func startSubscriberTestServer(t *testing.T, serve func(net.Conn) error) (string, <-chan error) {
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

func subscriberHandshake(conn net.Conn, clientID uint16) error {
	hello, err := readExpectedFrame(conn, protocol.TypeHello)
	if err != nil {
		return err
	}
	if hello.Version != protocol.Version || hello.ClientID != clientID {
		return errors.New("HELLO com versão ou client_id incorreto")
	}
	return answer(conn, protocol.Frame{Type: protocol.TypeStatus, Role: protocol.RolePrincipal, Epoch: 1})
}

func TestSubscriberReceivesMessageAndAcknowledges(t *testing.T) {
	ackSeen := make(chan protocol.Frame, 1)
	allowExit := make(chan struct{})
	address, serverDone := startSubscriberTestServer(t, func(conn net.Conn) error {
		if err := subscriberHandshake(conn, 77); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 9}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeSubscribe); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{
			Type: protocol.TypeMessage, TopicID: 9, DeliveryID: 1234,
			Payload: []byte{1, 2, 3, 4},
		}); err != nil {
			return err
		}
		ack, err := readExpectedFrame(conn, protocol.TypeDeliveryAck)
		if err != nil {
			return err
		}
		ackSeen <- ack
		<-allowExit
		return nil
	})

	subInterface, err := NewSubscriber(Config{Brokers: []string{address}, ClientID: 77})
	if err != nil {
		t.Fatalf("NewSubscriber(): %v", err)
	}
	sub := subInterface
	handled := make(chan []byte, 1)
	if err := sub.Subscribe("mar/temperatura", func(payload []byte) error {
		handled <- append([]byte(nil), payload...)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	select {
	case payload := <-handled:
		if string(payload) != string([]byte{1, 2, 3, 4}) {
			t.Fatalf("payload recebido incorreto: %v", payload)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("handler não recebeu a mensagem")
	}
	select {
	case ack := <-ackSeen:
		if ack.DeliveryID != 1234 {
			t.Fatalf("DELIVERY_ACK id=%d, esperado 1234", ack.DeliveryID)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("broker não recebeu DELIVERY_ACK")
	}

	close(allowExit)
	if err := sub.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestSubscriberHandlerErrorDoesNotAcknowledge(t *testing.T) {
	ackSeen := make(chan struct{}, 1)
	allowExit := make(chan struct{})
	address, serverDone := startSubscriberTestServer(t, func(conn net.Conn) error {
		if err := subscriberHandshake(conn, 78); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 10}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeSubscribe); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeMessage, TopicID: 10, DeliveryID: 55, Payload: []byte{9}}); err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		frame, err := protocol.ReadFrame(conn)
		if err == nil && frame.Type == protocol.TypeDeliveryAck {
			ackSeen <- struct{}{}
		}
		<-allowExit
		return nil
	})

	sub, err := NewSubscriber(Config{Brokers: []string{address}, ClientID: 78})
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan struct{}, 1)
	if err := sub.Subscribe("mar/temperatura", func([]byte) error {
		handled <- struct{}{}
		return errors.New("falha temporária no processamento")
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(4 * time.Second):
		t.Fatal("handler não foi chamado")
	}
	select {
	case <-ackSeen:
		t.Fatal("não deveria enviar DELIVERY_ACK quando handler retorna erro")
	case <-time.After(400 * time.Millisecond):
	}
	close(allowExit)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestSubscriberUnsubscribe(t *testing.T) {
	allowExit := make(chan struct{})
	address, serverDone := startSubscriberTestServer(t, func(conn net.Conn) error {
		if err := subscriberHandshake(conn, 79); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 11}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeSubscribe); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			return err
		}
		unsub, err := readExpectedFrame(conn, protocol.TypeUnsubscribe)
		if err != nil {
			return err
		}
		if unsub.TopicID != 11 {
			return errors.New("topic_id incorreto em UNSUBSCRIBE")
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			return err
		}
		<-allowExit
		return nil
	})

	sub, err := NewSubscriber(Config{Brokers: []string{address}, ClientID: 79})
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe("mar/temperatura", func([]byte) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe("mar/temperatura"); err != nil {
		t.Fatalf("Unsubscribe(): %v", err)
	}
	close(allowExit)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

func TestSubscriberRejectsInvalidSubscription(t *testing.T) {
	sub, err := NewSubscriber(Config{Brokers: []string{"127.0.0.1:1"}, ClientID: 80})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if err := sub.Subscribe("", func([]byte) error { return nil }); err == nil {
		t.Fatal("tópico vazio deveria ser rejeitado")
	}
	if err := sub.Subscribe("mar/temperatura", nil); err == nil {
		t.Fatal("handler nil deveria ser rejeitado")
	}
}

func TestSubscriberReconnectsAndResolvesChangedTopicID(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	ackSeen := make(chan protocol.Frame, 1)
	allowFirstDrop := make(chan struct{})

	go func() {
		first, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		if err := subscriberHandshake(first, 81); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if _, err := readExpectedFrame(first, protocol.TypeResolveTopic); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if err := answer(first, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 4}); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if _, err := readExpectedFrame(first, protocol.TypeSubscribe); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if err := answer(first, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		<-allowFirstDrop  // garante que Subscribe já recebeu OK antes da queda
		_ = first.Close() // simula queda do broker após a assinatura

		second, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer second.Close()
		hello, err := readExpectedFrame(second, protocol.TypeHello)
		if err != nil {
			serverDone <- err
			return
		}
		if hello.ClientID != 81 {
			serverDone <- errors.New("client_id mudou na reconexão")
			return
		}
		if err := answer(second, protocol.Frame{Type: protocol.TypeStatus, Role: protocol.RolePrincipal, Epoch: 2}); err != nil {
			serverDone <- err
			return
		}
		if _, err := readExpectedFrame(second, protocol.TypeResolveTopic); err != nil {
			serverDone <- err
			return
		}
		if err := answer(second, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 19}); err != nil {
			serverDone <- err
			return
		}
		restored, err := readExpectedFrame(second, protocol.TypeSubscribe)
		if err != nil {
			serverDone <- err
			return
		}
		if restored.TopicID != 19 {
			serverDone <- errors.New("assinatura não foi refeita com o novo topic_id")
			return
		}
		if err := answer(second, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			serverDone <- err
			return
		}
		if err := answer(second, protocol.Frame{Type: protocol.TypeMessage, TopicID: 19, DeliveryID: 88, Payload: []byte{8, 8}}); err != nil {
			serverDone <- err
			return
		}
		ack, err := readExpectedFrame(second, protocol.TypeDeliveryAck)
		if err == nil {
			ackSeen <- ack
		}
		serverDone <- err
	}()

	sub, err := NewSubscriber(Config{Brokers: []string{listener.Addr().String()}, ClientID: 81})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	handled := make(chan struct{}, 1)
	if err := sub.Subscribe("mar/temperatura", func([]byte) error {
		handled <- struct{}{}
		return nil
	}); err != nil {
		close(allowFirstDrop)
		t.Fatal(err)
	}
	close(allowFirstDrop)
	select {
	case <-handled:
	case <-time.After(8 * time.Second):
		t.Fatal("assinante não recebeu mensagem depois da reconexão")
	}
	select {
	case ack := <-ackSeen:
		if ack.DeliveryID != 88 {
			t.Fatalf("DELIVERY_ACK id=%d, esperado 88", ack.DeliveryID)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("broker não recebeu DELIVERY_ACK depois da reconexão")
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("broker falso: %v", err)
	}
}

// TestSubscriberBuffersEarlyMessagesAndBackpressuresBurst reproduz o caso em
// que o broker reenvia pendências imediatamente após HELLO e envia um backlog
// maior que a capacidade da fila local da assinatura.
func TestSubscriberBuffersEarlyMessagesAndBackpressuresBurst(t *testing.T) {
	const total = 70 // maior que a fila de 64 mensagens por assinatura
	handled := make(chan byte, total)

	address, serverDone := startSubscriberTestServer(t, func(conn net.Conn) error {
		if err := subscriberHandshake(conn, 82); err != nil {
			return err
		}

		// Entrega pendente chega antes de o cliente resolver o topic_id.
		if err := answer(conn, protocol.Frame{
			Type: protocol.TypeMessage, TopicID: 12, DeliveryID: 1, Payload: []byte{1},
		}); err != nil {
			return err
		}

		if _, err := readExpectedFrame(conn, protocol.TypeResolveTopic); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 12}); err != nil {
			return err
		}
		if _, err := readExpectedFrame(conn, protocol.TypeSubscribe); err != nil {
			return err
		}
		if err := answer(conn, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			return err
		}

		// Rajada superior à capacidade da fila. A leitora precisa aplicar
		// backpressure, não descartar as mensagens excedentes.
		for i := byte(2); i <= total; i++ {
			if err := answer(conn, protocol.Frame{
				Type: protocol.TypeMessage, TopicID: 12, DeliveryID: uint32(i), Payload: []byte{i},
			}); err != nil {
				return err
			}
		}
		for i := uint32(1); i <= total; i++ {
			ack, err := readExpectedFrame(conn, protocol.TypeDeliveryAck)
			if err != nil {
				return err
			}
			if ack.DeliveryID != i {
				return errors.New("DELIVERY_ACK recebido fora de ordem")
			}
		}
		return nil
	})

	sub, err := NewSubscriber(Config{Brokers: []string{address}, ClientID: 82})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := sub.Subscribe("mar/temperatura", func(payload []byte) error {
		// Torna o consumo deliberadamente mais lento que a chegada da rajada.
		time.Sleep(2 * time.Millisecond)
		if len(payload) != 1 {
			return errors.New("payload de teste inválido")
		}
		handled <- payload[0]
		return nil
	}); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	for expected := byte(1); expected <= total; expected++ {
		select {
		case got := <-handled:
			if got != expected {
				t.Fatalf("mensagem fora de ordem: recebeu %d, esperava %d", got, expected)
			}
		case <-time.After(8 * time.Second):
			t.Fatalf("mensagem %d não foi entregue", expected)
		}
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("broker falso: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("broker não recebeu todos os DELIVERY_ACKs")
	}
}

// TestSubscriberSameEpochKeepsTopicIDForEarlyRedelivery verifica que uma
// reconexão na mesma época pode reconhecer MESSAGE reenviado logo após STATUS,
// antes mesmo de RESOLVE_TOPIC terminar.
func TestSubscriberSameEpochKeepsTopicIDForEarlyRedelivery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	allowFirstDrop := make(chan struct{})
	handled := make(chan struct{}, 1)
	serverDone := make(chan error, 1)

	go func() {
		first, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		if err := subscriberHandshake(first, 83); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if _, err := readExpectedFrame(first, protocol.TypeResolveTopic); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if err := answer(first, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 14}); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if _, err := readExpectedFrame(first, protocol.TypeSubscribe); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		if err := answer(first, protocol.Frame{Type: protocol.TypeOK}); err != nil {
			_ = first.Close()
			serverDone <- err
			return
		}
		<-allowFirstDrop
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer second.Close()
		if _, err := readExpectedFrame(second, protocol.TypeHello); err != nil {
			serverDone <- err
			return
		}
		if err := answer(second, protocol.Frame{Type: protocol.TypeStatus, Role: protocol.RolePrincipal, Epoch: 1}); err != nil {
			serverDone <- err
			return
		}
		if err := answer(second, protocol.Frame{
			Type: protocol.TypeMessage, TopicID: 14, DeliveryID: 909, Payload: []byte{9},
		}); err != nil {
			serverDone <- err
			return
		}

		if _, err := readExpectedFrame(second, protocol.TypeResolveTopic); err != nil {
			serverDone <- err
			return
		}
		// O ACK precisa chegar antes de responder RESOLVE_TOPIC. Isso comprova
		// que o mapa byTopicID foi preservado na reconexão da mesma época.
		ack, err := readExpectedFrame(second, protocol.TypeDeliveryAck)
		if err != nil {
			serverDone <- err
			return
		}
		if ack.DeliveryID != 909 {
			serverDone <- errors.New("DELIVERY_ACK incorreto para entrega antecipada")
			return
		}
		if err := answer(second, protocol.Frame{Type: protocol.TypeTopicID, TopicID: 14}); err != nil {
			serverDone <- err
			return
		}
		if _, err := readExpectedFrame(second, protocol.TypeSubscribe); err != nil {
			serverDone <- err
			return
		}
		serverDone <- answer(second, protocol.Frame{Type: protocol.TypeOK})
	}()

	sub, err := NewSubscriber(Config{Brokers: []string{listener.Addr().String()}, ClientID: 83})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := sub.Subscribe("mar/temperatura", func([]byte) error {
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	close(allowFirstDrop)

	select {
	case <-handled:
	case <-time.After(8 * time.Second):
		t.Fatal("MESSAGE antecipado não foi entregue na reconexão da mesma época")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("broker falso: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("broker não concluiu a verificação da reconexão")
	}
}
