package cenarios

import (
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// 9.2 — Publicação sem assinantes: NO_SUBSCRIBERS; descarte conhecido pelo
// sensor (OnError), sem insistir.
func TestSemAssinantes(t *testing.T) {
	addr := freeAddr(t)
	broker(t, addr, server.RolePrincipal, t.TempDir(), 0)

	erros := make(chan error, 4)
	p := publicador(t, []string{addr}, 7, func(_ string, _ []byte, err error) { erros <- err })
	publicar(t, p, 1, 1)

	select {
	case err := <-erros:
		if !strings.Contains(err.Error(), "NO_SUBSCRIBERS") {
			t.Fatalf("esperado NO_SUBSCRIBERS, obtido %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("o sensor não foi avisado do descarte")
	}
	select {
	case err := <-erros:
		t.Fatalf("a mesma leitura foi tentada de novo: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
}

// 9.2 — Vários assinantes / um offline: entrega por destinatário; a
// pendência é preservada para o desconectado e chega quando ele volta.
func TestVariosAssinantesUmOffline(t *testing.T) {
	addr := freeAddr(t)
	broker(t, addr, server.RolePrincipal, t.TempDir(), 0)
	brokers := []string{addr}

	a, b := novoColetor(), novoColetor()
	assinante(t, brokers, 100, a.handle)
	subB := assinante(t, brokers, 101, b.handle)
	subB.Close() // B cai; a assinatura dele é durável

	p := publicador(t, brokers, 7, nil)
	publicar(t, p, 1, 20)
	a.esperar(t, 20, 10*time.Second)

	b2 := novoColetor()
	assinante(t, brokers, 101, b2.handle) // B volta com o mesmo client_id
	b2.esperar(t, 20, 10*time.Second)
}

// 9.2 — Falha e reinício do principal: o log local íntegro recompõe tópicos,
// assinaturas e pendências. Também cobre "sensor offline": as leituras
// feitas com o broker fora do ar ficam na fila local e são enviadas depois.
func TestReinicioDoPrincipal(t *testing.T) {
	addr, dados := freeAddr(t), t.TempDir()
	stop := broker(t, addr, server.RolePrincipal, dados, 0)
	brokers := []string{addr}

	c := novoColetor()
	sub := assinante(t, brokers, 100, c.handle)
	sub.Close() // assinante offline: as leituras ficam pendentes no broker

	p := publicador(t, brokers, 7, nil)
	publicar(t, p, 1, 10)
	time.Sleep(500 * time.Millisecond) // as 10 confirmadas (PUBLISH_ACK) e no log
	stop()                             // o principal cai com 10 entregas pendentes

	publicar(t, p, 11, 15) // sensor sem broker: vão para a fila local

	broker(t, addr, server.RolePrincipal, dados, 0) // reinicia com o mesmo disco
	c2 := novoColetor()
	assinante(t, brokers, 100, c2.handle)
	c2.esperar(t, 15, 15*time.Second)
}

// 9.2 — DELIVERY_ACK perdido: reentrega; o consumidor reconhece a repetição
// pelo payload. Aqui o handler falha na primeira vez de cada leitura (sem
// ACK); o broker reenvia depois do prazo de 5 s.
func TestDeliveryAckPerdido(t *testing.T) {
	addr := freeAddr(t)
	broker(t, addr, server.RolePrincipal, t.TempDir(), 0)
	brokers := []string{addr}

	c := novoColetor()
	var tentativas atomic.Int32
	vistos := make(map[uint32]bool)
	assinante(t, brokers, 100, func(p []byte) error {
		tentativas.Add(1)
		s := seqDe(p)
		if !vistos[s] { // handler de uma assinatura roda em uma goroutine só
			vistos[s] = true
			return io.ErrUnexpectedEOF // "falhou": sem DELIVERY_ACK
		}
		return c.handle(p)
	})

	p := publicador(t, brokers, 7, nil)
	publicar(t, p, 1, 3)
	c.esperar(t, 3, 12*time.Second)
	if n := tentativas.Load(); n < 6 {
		t.Fatalf("esperadas ao menos 6 entregas (3 + 3 reenvios), houve %d", n)
	}
}

// 9.2 — PUBLISH_ACK perdido: a leitura é reenviada, pode ser registrada duas
// vezes e é reconhecida pela aplicação. Um proxy derruba a conexão no
// primeiro PUBLISH_ACK, antes de ele chegar ao sensor.
func TestPublishAckPerdido(t *testing.T) {
	addr := freeAddr(t)
	broker(t, addr, server.RolePrincipal, t.TempDir(), 0)

	c := novoColetor()
	assinante(t, []string{addr}, 100, c.handle)

	proxy := proxyQuePerdeAck(t, addr)
	p := publicador(t, []string{proxy}, 7, nil)
	publicar(t, p, 1, 1)

	m := c.esperar(t, 1, 10*time.Second)
	fim := time.Now().Add(3 * time.Second)
	for m[1] < 2 && time.Now().Before(fim) {
		time.Sleep(50 * time.Millisecond)
		m = c.distintos()
	}
	if m[1] != 2 {
		t.Fatalf("esperada a leitura 1 duas vezes (registrada de novo após o ACK perdido), recebida %d", m[1])
	}
}

// 9.2 — Falha grave e promoção da reserva + publicação durante RECOVERING:
// os clientes trocam de broker, recriam tópico e assinatura (topic_id da
// nova época) e as leituras sem PUBLISH_ACK são reenviadas depois da janela.
func TestPromocaoDaReserva(t *testing.T) {
	principal, reserva := freeAddr(t), freeAddr(t)
	stopP := broker(t, principal, server.RolePrincipal, t.TempDir(), 0)
	broker(t, reserva, server.RoleReserva, t.TempDir(), 2*time.Second)
	brokers := []string{principal, reserva}

	c := novoColetor()
	assinante(t, brokers, 100, c.handle)
	p := publicador(t, brokers, 7, nil)
	publicar(t, p, 1, 10)
	c.esperar(t, 10, 10*time.Second)

	stopP() // falha grave do principal
	epoch, err := server.Promote(reserva)
	if err != nil {
		t.Fatal(err)
	}
	if epoch == 0 {
		t.Fatal("promoção sem época")
	}

	inicio := time.Now()
	publicar(t, p, 11, 20) // chegam durante RECOVERING: recusadas e reenviadas
	c.esperar(t, 20, 30*time.Second)
	if d := time.Since(inicio); d < 2*time.Second {
		t.Fatalf("leituras entregues em %s, antes do fim da janela RECOVERING de 2 s", d)
	}
}

// proxyQuePerdeAck repassa os bytes entre o sensor e o broker, mas no
// primeiro PUBLISH_ACK derruba a conexão em vez de repassá-lo.
func proxyQuePerdeAck(t *testing.T, broker string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var perdeu atomic.Bool
	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}
			srv, err := net.Dial("tcp", broker)
			if err != nil {
				cli.Close()
				continue
			}
			go func() { io.Copy(srv, cli); srv.Close() }()
			go func() {
				defer cli.Close()
				defer srv.Close()
				for {
					f, err := protocol.ReadFrame(srv)
					if err != nil {
						return
					}
					if f.Type == protocol.TypePublishAck && perdeu.CompareAndSwap(false, true) {
						return // o ACK "se perde" com a conexão
					}
					if err := protocol.WriteFrame(cli, f); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}
