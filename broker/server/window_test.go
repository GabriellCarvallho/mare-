package server

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// publishTagged publica uma leitura que diz de qual sensor e qual sequence
// ela é, para o teste conferir a ordem de chegada.
func (c *fakeClient) publishTagged(topic uint8, critical bool, sensor uint16, seq uint32) {
	c.t.Helper()
	p := make([]byte, 12)
	binary.BigEndian.PutUint16(p, sensor)
	binary.BigEndian.PutUint32(p[2:], seq)
	var flags byte
	if critical {
		flags = protocol.FlagCritical
	}
	c.send(protocol.Frame{Type: protocol.TypePublish, Flags: flags, TopicID: topic, Payload: p})
	expectPublishAck(c.t, c.recv())
}

// nextTag lê o próximo MESSAGE e devolve "C7-1" (crítica do sensor 7,
// leitura 1) ou "N7-1" (normal), além do delivery_id.
func nextTag(t *testing.T, c *fakeClient) (string, uint32) {
	t.Helper()
	f := c.recv()
	if f.Type != protocol.TypeMessage {
		t.Fatalf("esperado MESSAGE, recebido %v", f)
	}
	class := "N"
	if f.Critical() {
		class = "C"
	}
	return fmt.Sprintf("%s%d-%d", class, binary.BigEndian.Uint16(f.Payload), binary.BigEndian.Uint32(f.Payload[2:])), f.DeliveryID
}

// drain confirma uma entrega por vez e devolve a ordem em que chegaram.
func drain(t *testing.T, c *fakeClient, n int) string {
	t.Helper()
	var got []string
	for i := 0; i < n; i++ {
		tag, id := nextTag(t, c)
		got = append(got, tag)
		c.ack(id) // abre uma vaga na janela: o broker escolhe a próxima
	}
	return strings.Join(got, " ")
}

func windowBroker(t *testing.T, window int) (addr string, sub *fakeClient, b *broker) {
	t.Helper()
	cfg := fastConfig(nil)
	cfg.SendWindow = window
	cfg.onStart = func(x *broker) { b = x }
	addr = startBrokerWith(t, cfg)
	sub = helloed(t, addr, 100)
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	return addr, sub, b
}

// waitDispatched espera o despachante criar as entregas: o PUBLISH_ACK sai
// antes disso (a publicação já está no log e no escalonador), então sem a
// espera o teste poderia confirmar uma entrega antes de as outras existirem.
func waitDispatched(t *testing.T, b *broker, client uint16, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		got := len(b.dm.Waiting(client)) + b.dm.InFlight(client)
		b.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("entregas para o cliente %d: %d, esperado %d", client, got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// Seção 7 com consumidor lento: a janela segura as normais no broker, e as
// críticas que chegam depois passam na frente delas.
func TestJanelaCriticasPassamNaFrente(t *testing.T) {
	addr, sub, br := windowBroker(t, 2)
	a := helloed(t, addr, 7)
	for s := uint32(1); s <= 6; s++ {
		a.publishTagged(1, false, 7, s)
	}
	// Janela de 2: só as duas primeiras saem; o resto espera no broker.
	first, id1 := nextTag(t, sub)
	second, id2 := nextTag(t, sub)
	if first != "N7-1" || second != "N7-2" {
		t.Fatalf("primeiras entregas %s %s, esperado N7-1 N7-2", first, second)
	}
	expectNothing(t, sub)

	b := helloed(t, addr, 8)
	b.publishTagged(1, true, 8, 1)
	b.publishTagged(1, true, 8, 2)
	waitDispatched(t, br, 100, 8)

	sub.ack(id1)
	sub.ack(id2)
	if got, want := drain(t, sub, 6), "C8-1 C8-2 N7-3 N7-4 N7-5 N7-6"; got != want {
		t.Fatalf("ordem %q, esperado %q", got, want)
	}
}

// Rodízio entre sensores da mesma classe: um sensor que publicou antes não
// monopoliza o assinante.
func TestJanelaRodizioEntreSensores(t *testing.T) {
	addr, sub, br := windowBroker(t, 1)
	a, b := helloed(t, addr, 7), helloed(t, addr, 8)
	for s := uint32(1); s <= 3; s++ {
		a.publishTagged(1, false, 7, s)
	}
	waitDispatched(t, br, 100, 3)
	for s := uint32(1); s <= 3; s++ {
		b.publishTagged(1, false, 8, s)
	}
	waitDispatched(t, br, 100, 6)
	if got, want := drain(t, sub, 6), "N7-1 N8-1 N7-2 N8-2 N7-3 N8-3"; got != want {
		t.Fatalf("ordem %q, esperado %q", got, want)
	}
}

// Até 3 críticas para cada normal: as normais não ficam paradas para sempre.
func TestJanelaTresCriticasParaUmaNormal(t *testing.T) {
	addr, sub, br := windowBroker(t, 1)
	a, b := helloed(t, addr, 7), helloed(t, addr, 8)
	for s := uint32(1); s <= 4; s++ {
		a.publishTagged(1, false, 7, s)
	}
	waitDispatched(t, br, 100, 4) // N7-1 já saiu antes de as críticas chegarem
	for s := uint32(1); s <= 6; s++ {
		b.publishTagged(1, true, 8, s)
	}
	waitDispatched(t, br, 100, 10)
	want := "N7-1 C8-1 C8-2 C8-3 N7-2 C8-4 C8-5 C8-6 N7-3 N7-4"
	if got := drain(t, sub, 10); got != want {
		t.Fatalf("ordem %q, esperado %q", got, want)
	}
}
