// Package cenarios roda os testes da Seção 9.2 do documento de projeto de
// ponta a ponta: broker real (server.Run com o log em disco do wal), e a
// biblioteca cliente (client.Publisher / client.Subscriber) por TCP.
package cenarios

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/walstore"
	"github.com/GabriellCarvallho/mare-pubsub/client"
)

const topico = "mar/temperatura"

// freeAddr reserva um endereço local para o broker poder reiniciar na
// mesma porta (os clientes reconectam no mesmo endereço).
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// broker sobe um broker real com o log em disco em dataDir e devolve uma
// função que o derruba (como um Ctrl+C).
func broker(t *testing.T, addr, role, dataDir string, recovery time.Duration) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	up := make(chan struct{})
	done := make(chan error, 1)
	cfg := server.Config{
		Addr:          addr,
		Role:          role,
		DataDir:       dataDir,
		Recovery:      recovery,
		RetryInterval: 50 * time.Millisecond,
		OpenLog:       walstore.Open,
		OnListen:      func(net.Addr) { close(up) },
		Logf:          func(string, ...any) {},
	}
	go func() { done <- server.Run(ctx, cfg) }()
	select {
	case <-up:
	case err := <-done:
		t.Fatalf("broker %s não subiu: %v", role, err)
	case <-time.After(5 * time.Second):
		t.Fatalf("broker %s não subiu em 5 s", role)
	}
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("broker não encerrou em 5 s")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// leitura monta o payload de 12 bytes do sensor (Seção 3.3).
func leitura(sensor uint16, seq uint32) []byte {
	p := make([]byte, 12)
	binary.BigEndian.PutUint16(p[0:], sensor)
	binary.BigEndian.PutUint32(p[2:], seq)
	binary.BigEndian.PutUint32(p[6:], uint32(time.Now().Unix()))
	binary.BigEndian.PutUint16(p[10:], 2500)
	return p
}

func seqDe(p []byte) uint32 { return binary.BigEndian.Uint32(p[2:]) }

// coletor guarda o que o assinante recebeu (com repetições).
type coletor struct {
	mu       sync.Mutex
	recebido []uint32
	chegou   chan struct{}
}

func novoColetor() *coletor { return &coletor{chegou: make(chan struct{}, 1024)} }

func (c *coletor) handle(p []byte) error {
	c.mu.Lock()
	c.recebido = append(c.recebido, seqDe(p))
	c.mu.Unlock()
	select {
	case c.chegou <- struct{}{}:
	default:
	}
	return nil
}

// distintos devolve as sequências diferentes recebidas.
func (c *coletor) distintos() map[uint32]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := make(map[uint32]int)
	for _, s := range c.recebido {
		m[s]++
	}
	return m
}

// esperar espera até o coletor ter todas as sequências de 1 a n.
func (c *coletor) esperar(t *testing.T, n int, limite time.Duration) map[uint32]int {
	t.Helper()
	fim := time.Now().Add(limite)
	for {
		m := c.distintos()
		completo := true
		for s := uint32(1); s <= uint32(n); s++ {
			if m[s] == 0 {
				completo = false
				break
			}
		}
		if completo {
			return m
		}
		if time.Now().After(fim) {
			t.Fatalf("depois de %s, recebidas %d de %d leituras: %v", limite, len(m), n, m)
		}
		select {
		case <-c.chegou:
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func publicador(t *testing.T, brokers []string, id uint16, onError func(string, []byte, error)) client.Publisher {
	t.Helper()
	p, err := client.NewPublisher(client.Config{Brokers: brokers, ClientID: id, QueueDir: t.TempDir(), OnError: onError})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func assinante(t *testing.T, brokers []string, id uint16, handle func([]byte) error) client.Subscriber {
	t.Helper()
	s, err := client.NewSubscriber(client.Config{Brokers: brokers, ClientID: id})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Subscribe(topico, handle); err != nil {
		t.Fatal(err)
	}
	return s
}

func publicar(t *testing.T, p client.Publisher, de, ate uint32) {
	t.Helper()
	for s := de; s <= ate; s++ {
		if err := p.Publish(topico, s%4 == 0, leitura(7, s)); err != nil {
			t.Fatalf("Publish %d: %v", s, err)
		}
	}
}
