package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// syncBuffer: a saída do broker é escrita por outra goroutine.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// freeAddr devolve um endereço local com uma porta livre.
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

// startRun sobe o programa em segundo plano e espera a porta abrir.
func startRun(t *testing.T, args ...string) (out *syncBuffer, stop func() error) {
	t.Helper()
	out = &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, args, out, out) }()

	addr := args[indexOf(args, "-addr")+1]
	deadline := time.Now().Add(5 * time.Second)
	for {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		select {
		case err := <-done:
			t.Fatalf("broker terminou antes de abrir a porta: %v\n%s", err, out)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker não abriu %s em 5 s\n%s", addr, out)
		}
		time.Sleep(20 * time.Millisecond)
	}

	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel() // é o que o Ctrl+C faz no main
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("broker não encerrou em 5 s")
			return nil
		}
	}
	t.Cleanup(func() { stop() })
	return out, stop
}

func indexOf(list []string, s string) int {
	for i, x := range list {
		if x == s {
			return i
		}
	}
	return -1
}

func hello(t *testing.T, addr string) protocol.Frame {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := protocol.WriteFrame(conn, protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: 1}); err != nil {
		t.Fatal(err)
	}
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// O caminho completo de operação: principal e reserva sobem, o operador
// promove a reserva com -promote, e o Ctrl+C (cancelamento) encerra limpo.
func TestPrincipalReservaEPromocao(t *testing.T) {
	principal := freeAddr(t)
	reserva := freeAddr(t)

	_, stopP := startRun(t, "-role", "principal", "-addr", principal, "-data", t.TempDir())
	if f := hello(t, principal); f.Type != protocol.TypeStatus || f.Role != protocol.RolePrincipal {
		t.Fatalf("principal respondeu %v", f)
	}

	outR, _ := startRun(t, "-role", "reserva", "-addr", reserva, "-data", t.TempDir(), "-recovery", "0")
	if f := hello(t, reserva); f.Role != protocol.RoleReserva {
		t.Fatalf("reserva respondeu %v", f)
	}

	// O principal "cai" (Ctrl+C) e o operador promove a reserva.
	if err := stopP(); err != nil {
		t.Fatalf("encerramento do principal: %v", err)
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-promote", reserva}, &out, &out); err != nil {
		t.Fatalf("-promote: %v", err)
	}
	if !strings.Contains(out.String(), "promovido a principal") {
		t.Fatalf("saída do -promote: %q", out.String())
	}
	if f := hello(t, reserva); f.Role != protocol.RolePrincipal {
		t.Fatalf("depois da promoção a reserva respondeu %v", f)
	}
	if !strings.Contains(outR.String(), "promovido a principal") {
		t.Fatalf("o log da reserva não registrou a promoção:\n%s", outR)
	}

	// Promover de novo é recusado.
	if err := run(context.Background(), []string{"-promote", reserva}, &out, &out); err == nil {
		t.Fatal("segunda promoção deveria falhar")
	}
}

func TestOpcoesInvalidas(t *testing.T) {
	var out bytes.Buffer
	cases := [][]string{
		{"-role", "chefe", "-addr", "127.0.0.1:0"}, // papel inválido
		{"-addr"},                 // opção sem valor
		{"extra"},                 // argumento solto
		{"-promote", freeAddr(t)}, // ninguém escutando
	}
	for _, args := range cases {
		if err := run(context.Background(), args, &out, &out); err == nil {
			t.Errorf("run(%q) deveria falhar", args)
		}
	}
}

func TestAjuda(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-h"}, &out, &out); err != nil {
		t.Fatalf("-h devolveu erro: %v", err)
	}
	if !strings.Contains(out.String(), "-promote") {
		t.Fatalf("ajuda sem a opção -promote:\n%s", out.String())
	}
}
