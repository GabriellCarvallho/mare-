package server

import (
	"fmt"
	"testing"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// helloed devolve um cliente falso que já fez HELLO.
func helloed(t *testing.T, addr string, id uint16) *fakeClient {
	t.Helper()
	c := dial(t, addr)
	if f := c.hello(id); f.Type != protocol.TypeStatus {
		t.Fatalf("HELLO: %v", f)
	}
	return c
}

func (c *fakeClient) create(name string) protocol.Frame {
	c.t.Helper()
	c.send(protocol.Frame{Type: protocol.TypeCreateTopic, TopicName: name})
	return c.recv()
}

func (c *fakeClient) cmd(typ byte, id uint8) protocol.Frame {
	c.t.Helper()
	c.send(protocol.Frame{Type: typ, TopicID: id})
	return c.recv()
}

func expectTopicID(t *testing.T, f protocol.Frame, id uint8) {
	t.Helper()
	if f.Type != protocol.TypeTopicID || f.TopicID != id {
		t.Fatalf("esperado TOPIC_ID %d, recebido %v", id, f)
	}
}

func expectOK(t *testing.T, f protocol.Frame) {
	t.Helper()
	if f.Type != protocol.TypeOK {
		t.Fatalf("esperado OK, recebido %v", f)
	}
}

func TestCreateResolve(t *testing.T) {
	c := helloed(t, startBroker(t, RolePrincipal), 1)

	expectTopicID(t, c.create("mar/temp"), 1)
	expectTopicID(t, c.create("mar/sal"), 2)
	expectTopicID(t, c.create("mar/temp"), 1) // idempotente

	c.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/sal"})
	expectTopicID(t, c.recv(), 2)

	c.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "nao/existe"})
	expectError(t, c.recv(), protocol.CodeTopicNotFound)
}

// Tópicos são do broker, não da conexão: outro cliente vê o mesmo id.
func TestTopicoCompartilhadoEntreClientes(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	a := helloed(t, addr, 1)
	b := helloed(t, addr, 2)

	expectTopicID(t, a.create("mar/temp"), 1)
	b.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/temp"})
	expectTopicID(t, b.recv(), 1)
}

func TestLimite255PelaRede(t *testing.T) {
	c := helloed(t, startBroker(t, RolePrincipal), 1)
	for i := 1; i <= 255; i++ {
		expectTopicID(t, c.create(fmt.Sprintf("t%d", i)), uint8(i))
	}
	expectError(t, c.create("t256"), protocol.CodeTooManyTopics)
}

func TestSubscribeUnsubscribe(t *testing.T) {
	c := helloed(t, startBroker(t, RolePrincipal), 1)
	expectTopicID(t, c.create("mar/temp"), 1)

	expectOK(t, c.cmd(protocol.TypeSubscribe, 1))
	expectOK(t, c.cmd(protocol.TypeSubscribe, 1)) // idempotente
	expectOK(t, c.cmd(protocol.TypeUnsubscribe, 1))
	expectOK(t, c.cmd(protocol.TypeUnsubscribe, 1)) // idempotente

	expectError(t, c.cmd(protocol.TypeSubscribe, 99), protocol.CodeTopicNotFound)
}

// Sem pendências, DELETE remove o tópico de vez e libera o topic_id.
func TestDeleteSemPendencias(t *testing.T) {
	c := helloed(t, startBroker(t, RolePrincipal), 1)
	expectTopicID(t, c.create("mar/temp"), 1)

	expectOK(t, c.cmd(protocol.TypeDeleteTopic, 1))
	expectError(t, c.cmd(protocol.TypeSubscribe, 1), protocol.CodeTopicNotFound)
	expectError(t, c.cmd(protocol.TypeDeleteTopic, 1), protocol.CodeTopicNotFound)

	// O id 1 foi liberado e volta a ser usado.
	expectTopicID(t, c.create("mar/vento"), 1)
}

// A reserva responde HELLO, mas recusa o resto com NOT_LEADER.
func TestReservaRecusaComandos(t *testing.T) {
	c := helloed(t, startBroker(t, RoleReserva), 1)
	expectError(t, c.create("mar/temp"), protocol.CodeNotLeader)
	expectError(t, c.cmd(protocol.TypeSubscribe, 1), protocol.CodeNotLeader)
}
