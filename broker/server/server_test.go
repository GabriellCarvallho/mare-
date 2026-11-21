package server

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// startBroker sobe um broker numa porta livre e devolve o endereço.
// O broker é encerrado no fim do teste.
func startBroker(t *testing.T, role string) string {
	t.Helper()
	addrCh := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- Run(ctx, Config{
			Addr:     "127.0.0.1:0",
			Role:     role,
			OnListen: func(a net.Addr) { addrCh <- a.String() },
			Logf:     func(string, ...any) {},
		})
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run devolveu %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("broker não encerrou em 5 s")
		}
	})

	select {
	case a := <-addrCh:
		return a
	case err := <-done:
		t.Fatalf("broker não subiu: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("broker não subiu em 5 s")
	}
	return ""
}

// fakeClient é o "cliente falso" da divisão de tarefas: fala o protocolo
// direto, sem a biblioteca do Felipe.
type fakeClient struct {
	t    *testing.T
	conn net.Conn
}

func dial(t *testing.T, addr string) *fakeClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &fakeClient{t: t, conn: conn}
}

func (c *fakeClient) send(f protocol.Frame) {
	c.t.Helper()
	if err := protocol.WriteFrame(c.conn, f); err != nil {
		c.t.Fatal(err)
	}
}

func (c *fakeClient) recv() protocol.Frame {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	f, err := protocol.ReadFrame(c.conn)
	if err != nil {
		c.t.Fatalf("recv: %v", err)
	}
	return f
}

func (c *fakeClient) hello(id uint16) protocol.Frame {
	c.t.Helper()
	c.send(protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: id})
	return c.recv()
}

func expectError(t *testing.T, f protocol.Frame, code byte) {
	t.Helper()
	if f.Type != protocol.TypeError || f.ErrCode != code {
		t.Fatalf("esperado ERROR %s, recebido %v", protocol.CodeName(code), f)
	}
}

func TestHelloStatus(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	f := dial(t, addr).hello(7)
	if f.Type != protocol.TypeStatus || f.Role != protocol.RolePrincipal || f.Epoch == 0 {
		t.Fatalf("esperado STATUS principal, recebido %v", f)
	}
}

func TestReservaRespondeComoReserva(t *testing.T) {
	addr := startBroker(t, RoleReserva)
	f := dial(t, addr).hello(7)
	if f.Type != protocol.TypeStatus || f.Role != protocol.RoleReserva {
		t.Fatalf("esperado STATUS reserva, recebido %v", f)
	}
}

func TestComandoAntesDoHello(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	c := dial(t, addr)
	c.send(protocol.Frame{Type: protocol.TypeSubscribe, TopicID: 1})
	expectError(t, c.recv(), protocol.CodeHelloRequired)

	// A conexão continua viva: o HELLO depois funciona.
	if f := c.hello(7); f.Type != protocol.TypeStatus {
		t.Fatalf("HELLO depois do erro: %v", f)
	}
}

func TestVersaoNaoSuportada(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	c := dial(t, addr)
	c.send(protocol.Frame{Type: protocol.TypeHello, Version: 9, ClientID: 7})
	expectError(t, c.recv(), protocol.CodeUnsupportedVersion)
}

// Respostas saem na ordem dos pedidos, mesmo enviados de uma vez.
func TestRespostasEmOrdem(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	c := dial(t, addr)
	c.send(protocol.Frame{Type: protocol.TypeSubscribe, TopicID: 1}) // antes do HELLO
	c.send(protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: 7})
	expectError(t, c.recv(), protocol.CodeHelloRequired)
	if f := c.recv(); f.Type != protocol.TypeStatus {
		t.Fatalf("segunda resposta deveria ser STATUS: %v", f)
	}
}

// Quadro inválido derruba só aquela conexão; as outras seguem normais.
func TestQuadroInvalidoNaoDerrubaOutras(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	boa := dial(t, addr)
	boa.hello(1)

	ruim := dial(t, addr)
	ruim.conn.Write([]byte{0x00, 0x01, 0x7F}) // tipo 0x7F não existe
	expectError(t, ruim.recv(), protocol.CodeInvalidFrame)

	ruim.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := protocol.ReadFrame(ruim.conn); !errors.Is(err, io.EOF) {
		t.Fatalf("a conexão ruim deveria ser fechada, obtido %v", err)
	}

	// A boa continua respondendo.
	boa.send(protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: 1})
	if f := boa.recv(); f.Type != protocol.TypeStatus {
		t.Fatalf("conexão boa parou de responder: %v", f)
	}
}

func TestPapelInvalido(t *testing.T) {
	if err := Run(context.Background(), Config{Addr: "127.0.0.1:0", Role: "chefe"}); err == nil {
		t.Fatal("papel inválido deveria dar erro")
	}
}

// Encerrar o broker fecha as conexões abertas.
func TestEncerramentoFechaConexoes(t *testing.T) {
	addrCh := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{Addr: "127.0.0.1:0", Role: RolePrincipal,
			OnListen: func(a net.Addr) { addrCh <- a.String() }, Logf: func(string, ...any) {}})
	}()
	c := dial(t, <-addrCh)
	c.hello(1)

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := protocol.ReadFrame(c.conn); err == nil {
		t.Fatal("conexão deveria ter sido fechada no encerramento")
	}
}
