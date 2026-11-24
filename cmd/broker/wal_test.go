package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/wal"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

func TestConvErr(t *testing.T) {
	if convErr(nil) != nil {
		t.Fatal("nil deveria continuar nil")
	}
	if err := convErr(wal.ErrStorageFull); !errors.Is(err, server.ErrStorageFull) {
		t.Fatalf("ErrStorageFull virou %v", err)
	}
	if err := convErr(wal.ErrBroken); !errors.Is(err, server.ErrLogBroken) {
		t.Fatalf("ErrBroken virou %v", err)
	}
	outro := errors.New("outro")
	if convErr(outro) != outro {
		t.Fatal("erro desconhecido deveria passar sem mudança")
	}
}

// client é um cliente mínimo do protocolo para os testes do executável.
type client struct {
	t    *testing.T
	conn net.Conn
}

func connect(t *testing.T, addr string, id uint16) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	c := &client{t, conn}
	if f := c.do(protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: id}); f.Type != protocol.TypeStatus {
		t.Fatalf("HELLO respondeu %v", f)
	}
	return c
}

func (c *client) send(f protocol.Frame) {
	c.t.Helper()
	if err := protocol.WriteFrame(c.conn, f); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) recv() protocol.Frame {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(c.conn)
	if err != nil {
		c.t.Fatal(err)
	}
	return f
}

func (c *client) do(f protocol.Frame) protocol.Frame {
	c.t.Helper()
	c.send(f)
	return c.recv()
}

func expectType(t *testing.T, f protocol.Frame, typ byte) {
	t.Helper()
	if f.Type != typ {
		t.Fatalf("esperado %s, recebido %v", protocol.TypeName(typ), f)
	}
}

var leitura = []byte{0, 7, 0, 0, 1, 0x9C, 0x6A, 0xE1, 0xF1, 0xD0, 0x0A, 0x96}

// O caso da Seção 6.3 "principal reinicia com disco íntegro", agora com o
// log no disco de verdade: uma leitura confirmada e não entregue sobrevive
// ao encerramento do processo e chega ao assinante depois do reinício.
func TestReinicioComWALNoDisco(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)
	args := []string{"-role", "principal", "-addr", addr, "-data", dir}

	_, stop := startRun(t, args...)
	sub := connect(t, addr, 10)
	f := sub.do(protocol.Frame{Type: protocol.TypeCreateTopic, TopicName: "mar/temp"})
	expectType(t, f, protocol.TypeTopicID)
	expectType(t, sub.do(protocol.Frame{Type: protocol.TypeSubscribe, TopicID: f.TopicID}), protocol.TypeOK)
	sub.conn.Close() // assinante offline

	sensor := connect(t, addr, 7)
	expectType(t, sensor.do(protocol.Frame{Type: protocol.TypePublish, TopicID: f.TopicID, Payload: leitura}), protocol.TypePublishAck)
	if err := stop(); err != nil { // Ctrl+C
		t.Fatal(err)
	}

	// Os arquivos ficaram no disco: a época e o log dessa época.
	if _, err := os.Stat(filepath.Join(dir, "epoch")); err != nil {
		t.Fatalf("arquivo de época: %v", err)
	}
	logs, _ := filepath.Glob(filepath.Join(dir, "log-*.wal"))
	if len(logs) != 1 {
		t.Fatalf("esperado 1 arquivo de log, encontrado %v", logs)
	}

	// Reinicia com a mesma pasta: a leitura pendente é entregue.
	startRun(t, args...)
	sub2 := connect(t, addr, 10)
	msg := sub2.recv()
	expectType(t, msg, protocol.TypeMessage)
	if string(msg.Payload) != string(leitura) {
		t.Fatalf("payload diferente: % X", msg.Payload)
	}
	sub2.send(protocol.Frame{Type: protocol.TypeDeliveryAck, DeliveryID: msg.DeliveryID})

	// O tópico continua com o mesmo id.
	f2 := sub2.do(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/temp"})
	expectType(t, f2, protocol.TypeTopicID)
	if f2.TopicID != f.TopicID {
		t.Fatalf("topic_id mudou de %d para %d", f.TopicID, f2.TopicID)
	}
}
