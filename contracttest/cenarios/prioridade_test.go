package cenarios

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/sched"
	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// cru é uma conexão TCP sem a biblioteca: deixa o teste mandar quadros que a
// biblioteca não manda (rajada sem esperar ACK, DELETE_TOPIC).
type cru struct {
	t *testing.T
	c net.Conn
}

func conectarCru(t *testing.T, addr string, id uint16) *cru {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	r := &cru{t, c}
	r.send(protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: id})
	if f := r.recv(); f.Type != protocol.TypeStatus {
		t.Fatalf("HELLO: esperado STATUS, obtido %v", f)
	}
	return r
}

func (r *cru) send(f protocol.Frame) {
	r.t.Helper()
	if err := protocol.WriteFrame(r.c, f); err != nil {
		r.t.Fatal(err)
	}
}

func (r *cru) recv() protocol.Frame {
	r.t.Helper()
	r.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	f, err := protocol.ReadFrame(r.c)
	if err != nil {
		r.t.Fatal(err)
	}
	return f
}

func (r *cru) pedir(f protocol.Frame) protocol.Frame {
	r.t.Helper()
	r.send(f)
	return r.recv()
}

// ordem guarda a sequência em que as leituras chegaram ao assinante, por
// chave (sensor_id, sequence), sem repetições.
type ordem struct {
	mu    sync.Mutex
	pos   map[uint64]int
	lista []uint64
}

func novaOrdem() *ordem { return &ordem{pos: make(map[uint64]int)} }

func chave(sensor uint16, seq uint32) uint64 { return uint64(sensor)<<32 | uint64(seq) }

func (o *ordem) anotar(p []byte) {
	k := chave(binary.BigEndian.Uint16(p), binary.BigEndian.Uint32(p[2:]))
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.pos[k]; !ok {
		o.lista = append(o.lista, k)
		o.pos[k] = len(o.lista)
	}
}

func (o *ordem) tem(k uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.pos[k]
	return ok
}

// esperar aguarda todas as chaves; falha se faltar alguma no limite.
func (o *ordem) esperar(t *testing.T, chaves []uint64, limite time.Duration) {
	t.Helper()
	fim := time.Now().Add(limite)
	for _, k := range chaves {
		for !o.tem(k) {
			if time.Now().After(fim) {
				t.Fatalf("leitura %d/%d aceita pelo broker não chegou em %s", k>>32, uint32(k), limite)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// ultima devolve a posição da última leitura entregue de um sensor.
func (o *ordem) ultima(sensor uint16) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	u := 0
	for k, p := range o.pos {
		if uint16(k>>32) == sensor && p > u {
			u = p
		}
	}
	return u
}

// 9.2 — Sobrecarga e prioridades: um sensor que inunda o broker recebe
// OVERLOADED ao passar da sua cota e não impede os outros de publicar; com um
// consumidor lento, as críticas e as normais dos outros sensores são
// atendidas antes de o acúmulo do monopolizador acabar.
func TestSobrecargaEPrioridades(t *testing.T) {
	const cota, rajada, porSensor = 200, 600, 20
	addr := freeAddr(t)
	broker(t, addr, server.RolePrincipal, t.TempDir(), 0, func(c *server.Config) {
		c.Sched = sched.Config{PerPublisher: cota}
	})
	brokers := []string{addr}

	o := novaOrdem()
	assinante(t, brokers, 100, func(p []byte) error {
		o.anotar(p)
		time.Sleep(10 * time.Millisecond) // consumidor lento: forma fila no broker
		return nil
	})

	// Sensor 1, o monopolizador: manda a rajada inteira sem esperar ACK.
	mono := conectarCru(t, addr, 1)
	tf := mono.pedir(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: topico})
	if tf.Type != protocol.TypeTopicID {
		t.Fatalf("RESOLVE_TOPIC: %v", tf)
	}
	go func() {
		for s := uint32(1); s <= rajada; s++ {
			protocol.WriteFrame(mono.c, protocol.Frame{Type: protocol.TypePublish, TopicID: tf.TopicID, Payload: leitura(1, s)})
		}
	}()
	var aceitas []uint64
	recusadas := 0
	for s := uint32(1); s <= rajada; s++ { // respostas na ordem dos pedidos
		switch f := mono.recv(); {
		case f.Type == protocol.TypePublishAck:
			aceitas = append(aceitas, chave(1, s))
		case f.Type == protocol.TypeError && f.ErrCode == protocol.CodeOverloaded:
			recusadas++
		default:
			t.Fatalf("PUBLISH %d do monopolizador: resposta inesperada %v", s, f)
		}
	}
	if recusadas == 0 {
		t.Fatalf("o monopolizador mandou %d leituras e nenhuma recebeu OVERLOADED (cota %d)", rajada, cota)
	}
	if len(aceitas) < cota {
		t.Fatalf("só %d leituras do monopolizador aceitas, menos que a cota de %d", len(aceitas), cota)
	}

	// Sensores 2 (críticas) e 3 (normais), pela biblioteca, uma por vez.
	erros := make(chan error, 2*porSensor)
	onErr := func(_ string, _ []byte, err error) { erros <- err }
	criticas := publicador(t, brokers, 2, onErr)
	normais := publicador(t, brokers, 3, onErr)
	var wg sync.WaitGroup
	for _, s := range []struct {
		id      uint16
		critica bool
	}{{2, true}, {3, false}} {
		wg.Add(1)
		go func(id uint16, critica bool) {
			defer wg.Done()
			p := criticas
			if id == 3 {
				p = normais
			}
			for seq := uint32(1); seq <= porSensor; seq++ {
				if err := p.Publish(topico, critica, leitura(id, seq)); err != nil {
					erros <- err
				}
			}
		}(s.id, s.critica)
	}
	wg.Wait()

	todas := append([]uint64(nil), aceitas...)
	for seq := uint32(1); seq <= porSensor; seq++ {
		todas = append(todas, chave(2, seq), chave(3, seq))
	}
	o.esperar(t, todas, 60*time.Second)
	select {
	case err := <-erros:
		t.Fatalf("sensor fora da inundação recebeu erro: %v", err)
	default:
	}

	u1, u2, u3 := o.ultima(1), o.ultima(2), o.ultima(3)
	if u2 >= u1 {
		t.Errorf("críticas do sensor 2 terminaram na posição %d, depois do acúmulo do monopolizador (%d)", u2, u1)
	}
	if u3 >= u1 {
		t.Errorf("normais do sensor 3 terminaram na posição %d, depois do acúmulo do monopolizador (%d): sem rodízio", u3, u1)
	}
	if u2 >= u3 {
		t.Errorf("críticas do sensor 2 terminaram na posição %d, depois das normais do sensor 3 (%d)", u2, u3)
	}
	t.Logf("monopolizador: %d aceitas, %d OVERLOADED; última entrega: sensor 1 em %d, sensor 2 (críticas) em %d, sensor 3 em %d",
		len(aceitas), recusadas, u1, u2, u3)
}

// 9.2 — DELETE_TOPIC com pendências: o tópico passa a recusar novas
// publicações (TOPIC_REMOVING), mas as entregas já aceitas chegam ao
// assinante; só depois disso o tópico some e o topic_id é liberado.
func TestDeleteTopicComPendencias(t *testing.T) {
	addr := freeAddr(t)
	broker(t, addr, server.RolePrincipal, t.TempDir(), 0)
	brokers := []string{addr}

	// Assinante preso: segura as entregas no broker até ser liberado.
	libera := make(chan struct{})
	c := novoColetor()
	assinante(t, brokers, 100, func(p []byte) error {
		<-libera
		return c.handle(p)
	})

	op := conectarCru(t, addr, 7) // o sensor e, depois, o operador
	tf := op.pedir(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: topico})
	if tf.Type != protocol.TypeTopicID {
		t.Fatalf("RESOLVE_TOPIC: %v", tf)
	}
	for s := uint32(1); s <= 5; s++ { // 5 leituras aceitas (PUBLISH_ACK) antes do DELETE
		if f := op.pedir(protocol.Frame{Type: protocol.TypePublish, TopicID: tf.TopicID, Payload: leitura(7, s)}); f.Type != protocol.TypePublishAck {
			t.Fatalf("PUBLISH %d: %v", s, f)
		}
	}

	if f := op.pedir(protocol.Frame{Type: protocol.TypeDeleteTopic, TopicID: tf.TopicID}); f.Type != protocol.TypeOK {
		t.Fatalf("DELETE_TOPIC com pendências: esperado OK, obtido %v", f)
	}
	f := op.pedir(protocol.Frame{Type: protocol.TypePublish, TopicID: tf.TopicID, Payload: leitura(8, 1)})
	if f.Type != protocol.TypeError || f.ErrCode != protocol.CodeTopicRemoving {
		t.Fatalf("PUBLISH em tópico em remoção: esperado TOPIC_REMOVING, obtido %v", f)
	}
	if f := op.pedir(protocol.Frame{Type: protocol.TypeDeleteTopic, TopicID: tf.TopicID}); f.Type != protocol.TypeOK {
		t.Fatalf("o tópico sumiu antes das entregas pendentes: %v", f)
	}

	close(libera)
	c.esperar(t, 5, 15*time.Second) // as entregas aceitas antes do DELETE chegam

	// Sem pendências, o tópico é removido de vez.
	fim := time.Now().Add(10 * time.Second)
	for {
		f := op.pedir(protocol.Frame{Type: protocol.TypeDeleteTopic, TopicID: tf.TopicID})
		if f.Type == protocol.TypeError && f.ErrCode == protocol.CodeTopicNotFound {
			break
		}
		if time.Now().After(fim) {
			t.Fatalf("o tópico continua existindo depois das entregas: %v", f)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
