package server

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// reservaConfig: reserva com disco (DataDir) e relógio falso.
func reservaConfig(t *testing.T, clock *fakeClock, store *MemoryStore) Config {
	cfg := fastConfig(clock)
	cfg.Role = RoleReserva
	cfg.DataDir = t.TempDir()
	cfg.OpenLog = store.Open
	return cfg
}

func expectStatus(t *testing.T, f protocol.Frame, role byte) uint32 {
	t.Helper()
	if f.Type != protocol.TypeStatus || f.Role != role {
		t.Fatalf("esperado STATUS papel=%d, recebido %v", role, f)
	}
	return f.Epoch
}

// Seção 9.2: falha grave e promoção da reserva + publicação durante
// RECOVERING. A reserva começa vazia, recusa tudo com NOT_LEADER; depois do
// PROMOTE vira principal com época maior; PUBLISH recebe RECOVERING até a
// janela fechar; depois disso funciona normalmente.
func TestPromocaoDaReserva(t *testing.T) {
	clock := newFakeClock()
	cfg := reservaConfig(t, clock, NewMemoryStore(0))
	addr, _ := runBroker(t, cfg)

	antes := dial(t, addr)
	if e := expectStatus(t, antes.hello(10), protocol.RoleReserva); e != 0 {
		t.Fatalf("reserva sem disco anterior deveria ter época 0, tem %d", e)
	}
	expectError(t, antes.create("mar/temp"), protocol.CodeNotLeader)

	epoch, err := Promote(addr)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint32(clock.Now().Unix()); epoch != want {
		t.Fatalf("época %d, esperado %d (segundos Unix)", epoch, want)
	}
	if e, ok, _ := readEpoch(cfg.DataDir); !ok || e != epoch {
		t.Fatalf("arquivo de época com %d, esperado %d", e, epoch)
	}

	// Clientes novos veem o principal com a época nova.
	sub := dial(t, addr)
	if e := expectStatus(t, sub.hello(10), protocol.RolePrincipal); e != epoch {
		t.Fatalf("STATUS com época %d, esperado %d", e, epoch)
	}
	// Durante a janela: tópicos e assinaturas sim, PUBLISH não.
	expectTopicID(t, sub.create("mar/temp"), 1)
	expectOK(t, sub.cmd(protocol.TypeSubscribe, 1))
	sensor := helloed(t, addr, 7)
	expectError(t, sensor.publish(1, false), protocol.CodeRecovering)

	// A conexão aberta antes da promoção também passa a ser atendida.
	expectTopicID(t, antes.create("mar/temp"), 1)

	// Fechou a janela: a leitura reenviada é aceita e entregue.
	clock.Advance(DefaultRecovery + time.Second)
	expectPublishAck(t, sensor.publish(1, false))
	sub.ack(expectMessage(t, sub, 1).DeliveryID)

	// Promover de novo não faz nada.
	if _, err := Promote(addr); !errors.Is(err, ErrAlreadyPrincipal) {
		t.Fatalf("segunda promoção: esperado ErrAlreadyPrincipal, obtido %v", err)
	}
}

func TestPromoverPrincipal(t *testing.T) {
	addr := startBroker(t, RolePrincipal)
	if _, err := Promote(addr); !errors.Is(err, ErrAlreadyPrincipal) {
		t.Fatalf("esperado ErrAlreadyPrincipal, obtido %v", err)
	}
}

func TestPromoverInacessivel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // porta sem ninguém ouvindo

	if _, err := Promote(addr); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("esperado ErrUnreachable, obtido %v", err)
	}
}

// A época nunca diminui: se o disco já tem uma época maior que o relógio
// (por exemplo, relógio atrasado), a promoção usa a anterior + 1.
func TestEpocaNuncaDiminui(t *testing.T) {
	clock := newFakeClock()
	cfg := reservaConfig(t, clock, NewMemoryStore(0))
	const alta = 4_000_000_000 // bem maior que os segundos Unix do relógio
	if err := writeEpoch(cfg.DataDir, alta); err != nil {
		t.Fatal(err)
	}
	addr, _ := runBroker(t, cfg)

	c := dial(t, addr)
	if e := expectStatus(t, c.hello(1), protocol.RoleReserva); e != alta {
		t.Fatalf("reserva informa época %d, esperado %d", e, alta)
	}
	epoch, err := Promote(addr)
	if err != nil {
		t.Fatal(err)
	}
	if epoch != alta+1 {
		t.Fatalf("época %d, esperado %d", epoch, alta+1)
	}
}

// Depois de promovida, se a nova principal reiniciar com o disco íntegro,
// ela volta com a MESMA época e o mesmo log (Seção 6.3: "principal reinicia
// com disco íntegro"), sem janela RECOVERING.
func TestPromovidaReiniciaComoPrincipal(t *testing.T) {
	clock := newFakeClock()
	store := NewMemoryStore(0)
	cfg := reservaConfig(t, clock, store)
	addr, stop := runBroker(t, cfg)

	epoch, err := Promote(addr)
	if err != nil {
		t.Fatal(err)
	}
	c := helloed(t, addr, 10)
	expectTopicID(t, c.create("a"), 1)
	expectTopicID(t, c.create("mar/temp"), 2)
	expectOK(t, c.cmd(protocol.TypeSubscribe, 2))
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	cfg.Role = RolePrincipal // o operador agora a sobe como principal
	addr, _ = runBroker(t, cfg)
	c = dial(t, addr)
	if e := expectStatus(t, c.hello(10), protocol.RolePrincipal); e != epoch {
		t.Fatalf("reiniciou com época %d, esperado %d", e, epoch)
	}
	c.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/temp"})
	expectTopicID(t, c.recv(), 2)
	sensor := helloed(t, addr, 7)
	expectPublishAck(t, sensor.publish(2, false)) // sem RECOVERING
	expectMessage(t, c, 2)
}

// O principal antigo, ao voltar como reserva, ignora o log antigo e
// continua informando a época antiga (menor): os clientes não voltam para
// ele (Seção 6.2, item 5).
func TestPrincipalAntigoVoltaComoReserva(t *testing.T) {
	clock := newFakeClock()
	store := NewMemoryStore(0)
	cfg := persistentConfig(t, store)
	cfg.Now = clock.Now
	addr, stop := runBroker(t, cfg)
	c := helloed(t, addr, 10)
	expectTopicID(t, c.create("mar/temp"), 1)
	old, _, _ := readEpoch(cfg.DataDir)
	stop()

	cfg.Role = RoleReserva
	addr, _ = runBroker(t, cfg)
	c = dial(t, addr)
	if e := expectStatus(t, c.hello(10), protocol.RoleReserva); e != old {
		t.Fatalf("reserva informa época %d, esperado %d", e, old)
	}
	expectError(t, c.create("mar/temp"), protocol.CodeNotLeader)
}

// O principal cria a época na primeira vez, grava no disco e, ao reiniciar
// com o mesmo disco, volta com a MESMA época (e o mesmo log).
func TestReinicioMantemEpoca(t *testing.T) {
	clock := newFakeClock()
	cfg := persistentConfig(t, NewMemoryStore(0))
	cfg.Now = clock.Now

	addr, stop := runBroker(t, cfg)
	primeira := expectStatus(t, dial(t, addr).hello(1), protocol.RolePrincipal)
	if want := uint32(clock.Now().Unix()); primeira != want {
		t.Fatalf("época %d, esperado %d (segundos Unix)", primeira, want)
	}
	if e, ok, _ := readEpoch(cfg.DataDir); !ok || e != primeira {
		t.Fatalf("arquivo de época com %d, esperado %d", e, primeira)
	}
	stop()

	clock.Advance(time.Hour) // mesmo com o relógio andando, a época não muda
	addr, _ = runBroker(t, cfg)
	if e := expectStatus(t, dial(t, addr).hello(1), protocol.RolePrincipal); e != primeira {
		t.Fatalf("reiniciou com época %d, esperado %d", e, primeira)
	}
}

// A reserva não grava nada; só informa a época que já estava no disco.
func TestReservaInformaEpocaDoDisco(t *testing.T) {
	cfg := fastConfig(nil)
	cfg.Role = RoleReserva
	cfg.DataDir = t.TempDir()

	addr, stop := runBroker(t, cfg)
	if e := expectStatus(t, dial(t, addr).hello(1), protocol.RoleReserva); e != 0 {
		t.Fatalf("reserva sem disco anterior: época %d, esperado 0", e)
	}
	stop()
	if _, ok, _ := readEpoch(cfg.DataDir); ok {
		t.Fatal("a reserva não deveria gravar a época")
	}

	writeEpoch(cfg.DataDir, 42)
	addr, _ = runBroker(t, cfg)
	if e := expectStatus(t, dial(t, addr).hello(1), protocol.RoleReserva); e != 42 {
		t.Fatalf("reserva informa época %d, esperado 42", e)
	}
}

func TestNextEpoch(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cases := []struct{ prev, want uint32 }{
		{0, 1_800_000_000},
		{5, 1_800_000_000},
		{1_800_000_000, 1_800_000_001},
		{2_000_000_000, 2_000_000_001},
	}
	for _, c := range cases {
		if got := nextEpoch(c.prev, now); got != c.want {
			t.Errorf("nextEpoch(%d) = %d, esperado %d", c.prev, got, c.want)
		}
	}
}

func TestArquivoDeEpocaInvalido(t *testing.T) {
	cfg := fastConfig(nil)
	cfg.DataDir = t.TempDir()
	if err := writeEpoch(cfg.DataDir, 7); err != nil {
		t.Fatal(err)
	}
	if e, ok, err := readEpoch(cfg.DataDir); err != nil || !ok || e != 7 {
		t.Fatalf("readEpoch = %d, %v, %v", e, ok, err)
	}
	// Conteúdo corrompido: o broker não sobe (não inventa uma época).
	if err := os.WriteFile(filepath.Join(cfg.DataDir, "epoch"), []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg.Addr = "127.0.0.1:0"
	cfg.Logf = func(string, ...any) {}
	if err := Run(t.Context(), cfg); err == nil {
		t.Fatal("broker subiu com arquivo de época inválido")
	}
}
