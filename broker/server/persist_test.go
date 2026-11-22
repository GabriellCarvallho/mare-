package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// runBroker sobe um broker e devolve o endereço e uma função que o derruba
// (e devolve o erro do Run). Usado para simular reinícios.
func runBroker(t *testing.T, cfg Config) (addr string, stop func() error) {
	t.Helper()
	addrCh := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	cfg.Addr = "127.0.0.1:0"
	cfg.OnListen = func(a net.Addr) { addrCh <- a.String() }
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	go func() { done <- Run(ctx, cfg) }()

	select {
	case addr = <-addrCh:
	case err := <-done:
		t.Fatalf("broker não subiu: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("broker não subiu em 5 s")
	}

	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("broker não encerrou em 5 s")
			return nil
		}
	}
	t.Cleanup(func() { stop() })
	return addr, stop
}

// persistentConfig: o mesmo MemoryStore e o mesmo DataDir entre reinícios,
// como se fosse o mesmo disco.
func persistentConfig(t *testing.T, store *MemoryStore) Config {
	cfg := fastConfig(nil)
	cfg.DataDir = t.TempDir()
	cfg.OpenLog = store.Open
	return cfg
}

// Seção 9.2: falha e reinício do principal. O log recompõe tópicos e
// assinaturas: o tópico tem o mesmo id e o assinante continua inscrito.
func TestReinicioRecuperaTopicosEAssinaturas(t *testing.T) {
	store := NewMemoryStore(0)
	cfg := persistentConfig(t, store)

	addr, stop := runBroker(t, cfg)
	admin := helloed(t, addr, 1)
	expectTopicID(t, admin.create("mar/temp"), 1)
	expectTopicID(t, admin.create("mar/sal"), 2)
	sub := helloed(t, addr, 10)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 2))
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	addr, _ = runBroker(t, cfg) // "reinicia" com o mesmo log
	c := helloed(t, addr, 1)
	c.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/sal"})
	expectTopicID(t, c.recv(), 2) // mesmo id de antes

	// A assinatura do 10 sobreviveu: publicar no tópico 2 é aceito.
	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(2, false))
	// E no tópico 1, sem assinantes, continua NO_SUBSCRIBERS.
	expectError(t, sensor.publish(1, false), protocol.CodeNoSubscribers)
}

// Uma publicação confirmada (PUBLISH_ACK) e ainda não entregue sobrevive ao
// reinício e chega ao assinante quando ele conecta.
func TestReinicioRecuperaPendencias(t *testing.T) {
	store := NewMemoryStore(0)
	cfg := persistentConfig(t, store)

	addr, stop := runBroker(t, cfg)
	admin := helloed(t, addr, 1)
	expectTopicID(t, admin.create("mar/temp"), 1)
	sub := helloed(t, addr, 10)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sub.conn.Close() // assinante offline

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))
	if err := stop(); err != nil { // o broker cai com a entrega pendente
		t.Fatal(err)
	}

	addr, _ = runBroker(t, cfg)
	sub2 := helloed(t, addr, 10)
	msg := expectMessage(t, sub2, 1) // chega depois do reinício
	sub2.ack(msg.DeliveryID)
}

// Publicação já confirmada pelo assinante não é reenviada depois do reinício.
func TestReinicioNaoReenviaConcluidas(t *testing.T) {
	store := NewMemoryStore(0)
	cfg := persistentConfig(t, store)

	addr, stop := runBroker(t, cfg)
	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))
	msg := expectMessage(t, sub, 1)
	sub.ack(msg.DeliveryID)
	// Sincroniza: um pedido depois do ACK, na mesma conexão, garante que o
	// broker já processou o ACK (respostas em ordem).
	expectTopicID(t, sub.create("mar/temp"), 1)
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	addr, _ = runBroker(t, cfg)
	sub2 := helloed(t, addr, 10)
	expectNothing(t, sub2)
}

// DELETE_TOPIC com pendências sobrevive ao reinício: o tópico continua em
// remoção até a última entrega.
func TestReinicioMantemTopicoEmRemocao(t *testing.T) {
	store := NewMemoryStore(0)
	cfg := persistentConfig(t, store)

	addr, stop := runBroker(t, cfg)
	admin := helloed(t, addr, 1)
	expectTopicID(t, admin.create("mar/temp"), 1)
	sub := helloed(t, addr, 10)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sub.conn.Close()
	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))
	expectOK(t, admin.cmd(protocol.TypeDeleteTopic, 1))
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	addr, _ = runBroker(t, cfg)
	admin2 := helloed(t, addr, 1)
	expectError(t, admin2.create("mar/temp"), protocol.CodeTopicRemoving)

	sub2 := helloed(t, addr, 10)
	msg := expectMessage(t, sub2, 1)
	sub2.ack(msg.DeliveryID)
	expectTopicID(t, sub2.create("mar/vento"), 1) // removido e id liberado
}

// Log cheio: a publicação não é confirmada (STORAGE_UNAVAILABLE).
func TestLogCheio(t *testing.T) {
	store := NewMemoryStore(200) // cabem só alguns registros
	cfg := persistentConfig(t, store)
	addr, _ := runBroker(t, cfg)

	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sub.conn.Close() // ninguém confirma: nada é compactado

	sensor := helloed(t, addr, 7)
	for i := 0; ; i++ {
		f := sensor.publish(1, false)
		if f.Type == protocol.TypeError {
			expectError(t, f, protocol.CodeStorageUnavailable)
			return
		}
		expectPublishAck(t, f)
		if i > 50 {
			t.Fatal("o log deveria ter enchido")
		}
	}
}

// brokenLog simula um disco com defeito: toda gravação falha.
type brokenLog struct{}

func (brokenLog) Append(Record) (uint64, error)           { return 0, ErrLogBroken }
func (brokenLog) Replay(func(uint64, Record) error) error { return nil }
func (brokenLog) Compact(func(uint64, Record) bool) error { return nil }
func (brokenLog) Close() error                            { return nil }

// Log quebrado: o broker não pode seguir confirmando e o Run termina com erro.
func TestLogQuebradoEncerraBroker(t *testing.T) {
	cfg := fastConfig(nil)
	cfg.DataDir = t.TempDir()
	cfg.OpenLog = func(string) (Log, error) { return brokenLog{}, nil }
	cfg.Addr = "127.0.0.1:0"
	cfg.Logf = func(string, ...any) {}

	err := Run(context.Background(), cfg) // a gravação do EPOCH já falha
	if !errors.Is(err, ErrLogBroken) {
		t.Fatalf("esperado ErrLogBroken, obtido %v", err)
	}
}

// Compactação: publicações concluídas saem do log.
func TestCompactacao(t *testing.T) {
	store := NewMemoryStore(0)
	cfg := persistentConfig(t, store)
	cfg.CompactEvery = 3
	addr, _ := runBroker(t, cfg)

	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sensor := helloed(t, addr, 7)

	for i := 0; i < 3; i++ {
		expectPublishAck(t, sensor.publish(1, false))
		sub.ack(expectMessage(t, sub, 1).DeliveryID)
	}
	expectTopicID(t, sub.create("mar/temp"), 1) // sincroniza com os ACKs

	// Sobram EPOCH, TOPIC_CREATED e SUBSCRIBED; os 3 PUBLISHED e os 3
	// DELIVERED foram removidos.
	if n := store.Len(logPath(cfg.DataDir, 1)); n != 3 {
		t.Fatalf("log com %d registros depois da compactação, esperado 3", n)
	}
}
