package server

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/sched"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// fakeClock é um relógio que só anda quando o teste manda. Assim dá para
// testar o prazo de 5 s do ACK sem esperar 5 s de verdade.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fastConfig: relógio de reenvio rápido para os testes não demorarem.
func fastConfig(clock *fakeClock) Config {
	cfg := Config{Role: RolePrincipal, RetryInterval: 10 * time.Millisecond}
	if clock != nil {
		cfg.Now = clock.Now
	}
	return cfg
}

var leitura = []byte{0, 7, 0, 0, 1, 0x9C, 0x6A, 0xE1, 0xF1, 0xD0, 0x0A, 0x96} // payload de 12 bytes

func (c *fakeClient) publish(topic uint8, critical bool) protocol.Frame {
	c.t.Helper()
	var flags byte
	if critical {
		flags = protocol.FlagCritical
	}
	c.send(protocol.Frame{Type: protocol.TypePublish, Flags: flags, TopicID: topic, Payload: leitura})
	return c.recv()
}

func (c *fakeClient) ack(id uint32) {
	c.t.Helper()
	c.send(protocol.Frame{Type: protocol.TypeDeliveryAck, DeliveryID: id})
}

// expectMessage lê o próximo quadro e confere que é um MESSAGE do tópico.
func expectMessage(t *testing.T, c *fakeClient, topic uint8) protocol.Frame {
	t.Helper()
	f := c.recv()
	if f.Type != protocol.TypeMessage || f.TopicID != topic || f.DeliveryID == 0 ||
		!bytes.Equal(f.Payload, leitura) {
		t.Fatalf("esperado MESSAGE do tópico %d com a leitura, recebido %v", topic, f)
	}
	return f
}

// expectNothing confere que nada chega por um tempo curto.
func expectNothing(t *testing.T, c *fakeClient) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if f, err := protocol.ReadFrame(c.conn); err == nil {
		t.Fatalf("não deveria chegar nada, chegou %v", f)
	}
}

func expectPublishAck(t *testing.T, f protocol.Frame) {
	t.Helper()
	if f.Type != protocol.TypePublishAck {
		t.Fatalf("esperado PUBLISH_ACK, recebido %v", f)
	}
}

// Seção 9.2: publicação sem assinantes.
func TestPublishSemAssinantes(t *testing.T) {
	c := helloed(t, startBrokerWith(t, fastConfig(nil)), 7)
	expectTopicID(t, c.create("mar/temp"), 1)

	expectError(t, c.publish(1, false), protocol.CodeNoSubscribers)
	expectError(t, c.publish(99, false), protocol.CodeTopicNotFound)
}

// Fluxo completo: PUBLISH → PUBLISH_ACK → MESSAGE → DELIVERY_ACK.
func TestPublishEntregaEConfirma(t *testing.T) {
	addr := startBrokerWith(t, fastConfig(nil))
	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, true))

	msg := expectMessage(t, sub, 1)
	if !msg.Critical() {
		t.Fatal("a flag CRÍTICA deveria chegar ao assinante")
	}
	sub.ack(msg.DeliveryID)

	// Publicação concluída: DELETE remove na hora e o id 1 volta a ser usado.
	// (ACK e DELETE na mesma conexão: o broker processa em ordem.)
	expectOK(t, sub.cmd(protocol.TypeDeleteTopic, 1))
	expectTopicID(t, sub.create("mar/vento"), 1)
}

// Seção 9.2: vários assinantes, um offline. O online recebe na hora; o
// offline recebe quando volta, sem precisar assinar de novo.
func TestDoisAssinantesUmOffline(t *testing.T) {
	addr := startBrokerWith(t, fastConfig(nil))
	a := helloed(t, addr, 10)
	expectTopicID(t, a.create("mar/temp"), 1)
	expectOK(t, a.cmd(protocol.TypeSubscribe, 1))

	b := helloed(t, addr, 20)
	expectOK(t, b.cmd(protocol.TypeSubscribe, 1))
	b.conn.Close() // o 20 cai; a assinatura continua valendo

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))

	expectMessage(t, a, 1)

	b2 := helloed(t, addr, 20) // volta com o mesmo client_id
	expectMessage(t, b2, 1)
}

// Seção 9.2: DELIVERY_ACK perdido → reentrega com o mesmo delivery_id.
func TestAckPerdidoReenvia(t *testing.T) {
	clock := newFakeClock()
	addr := startBrokerWith(t, fastConfig(clock))
	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))

	first := expectMessage(t, sub, 1) // não confirma: o ACK "se perdeu"
	expectNothing(t, sub)             // antes de 5 s, nada de reenvio

	clock.Advance(5 * time.Second)
	again := expectMessage(t, sub, 1)
	if again.DeliveryID != first.DeliveryID {
		t.Fatalf("reenvio deveria ter o mesmo delivery_id: %d != %d", again.DeliveryID, first.DeliveryID)
	}

	sub.ack(again.DeliveryID)
	clock.Advance(time.Minute)
	expectNothing(t, sub) // confirmado: não reenvia mais
}

// Seção 9.2: DELETE_TOPIC com pendências bloqueia o tópico, mas mantém as
// entregas já aceitas; o topic_id só é liberado depois delas.
func TestDeleteComPendencias(t *testing.T) {
	addr := startBrokerWith(t, fastConfig(nil))
	admin := helloed(t, addr, 1)
	expectTopicID(t, admin.create("mar/temp"), 1)

	sub := helloed(t, addr, 10)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sub.conn.Close() // assinante offline: a entrega fica pendente

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))

	expectOK(t, admin.cmd(protocol.TypeDeleteTopic, 1))
	expectError(t, sensor.publish(1, false), protocol.CodeTopicRemoving) // recusa novas
	expectError(t, admin.create("mar/temp"), protocol.CodeTopicRemoving)
	expectTopicID(t, admin.create("mar/sal"), 2) // o id 1 continua ocupado

	sub2 := helloed(t, addr, 10)
	msg := expectMessage(t, sub2, 1) // a entrega aceita antes do DELETE chega
	sub2.ack(msg.DeliveryID)

	// Agora sem pendências: o tópico foi removido e o id 1 está livre.
	expectTopicID(t, sub2.create("mar/vento"), 1)
}

// Seção 9.2: sobrecarga. Acima da cota, OVERLOADED; os outros sensores
// continuam sendo aceitos.
func TestOverloaded(t *testing.T) {
	cfg := fastConfig(nil)
	cfg.Sched = sched.Config{PerPublisher: 2}
	addr := startBrokerWith(t, cfg)

	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sub.conn.Close() // ninguém confirma: as publicações ficam pendentes

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))
	expectPublishAck(t, sensor.publish(1, false))
	expectError(t, sensor.publish(1, false), protocol.CodeOverloaded)

	outro := helloed(t, addr, 8)
	expectPublishAck(t, outro.publish(1, false))
}

// Quarentena: depois de 10 falhas seguidas a entrega para de ser tentada e
// a cota do sensor é liberada.
func TestQuarentenaLiberaCota(t *testing.T) {
	clock := newFakeClock()
	cfg := fastConfig(clock)
	cfg.Sched = sched.Config{PerPublisher: 1}
	addr := startBrokerWith(t, cfg)

	sub := helloed(t, addr, 10)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))

	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(1, false))
	expectError(t, sensor.publish(1, false), protocol.CodeOverloaded) // cota de 1

	expectMessage(t, sub, 1) // envio inicial; o assinante nunca confirma
	prazo := 5 * time.Second
	for falha := 1; falha <= 9; falha++ {
		clock.Advance(prazo)
		expectMessage(t, sub, 1) // reenvio depois de cada prazo
		prazo = min(prazo*2, 60*time.Second)
	}
	clock.Advance(prazo) // 10ª falha: quarentena, sem reenvio

	// A cota é liberada assim que o relógio processar a quarentena.
	deadline := time.Now().Add(2 * time.Second)
	for {
		f := sensor.publish(1, false)
		if f.Type == protocol.TypePublishAck {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cota não foi liberada após a quarentena: %v", f)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Rajada com assinante offline: as 8 entregas acumulam e todas chegam
// quando ele volta. (A ordem 3 críticas : 1 normal é testada no pacote sched.)
func TestRajadaAcumuladaChegaInteira(t *testing.T) {
	addr := startBrokerWith(t, fastConfig(nil))
	admin := helloed(t, addr, 1)
	expectTopicID(t, admin.create("mar/temp"), 1)

	sub := helloed(t, addr, 10)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sub.conn.Close() // offline: as entregas acumulam

	sensor := helloed(t, addr, 7)
	for i := 0; i < 4; i++ {
		expectPublishAck(t, sensor.publish(1, false))
	}
	for i := 0; i < 4; i++ {
		expectPublishAck(t, sensor.publish(1, true))
	}

	// Ao voltar, recebe as 8 entregas.
	sub2 := helloed(t, addr, 10)
	got := 0
	for got < 8 {
		msg := expectMessage(t, sub2, 1)
		sub2.ack(msg.DeliveryID)
		got++
	}
}
