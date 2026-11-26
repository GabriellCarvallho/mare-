package delivery

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)

func online(uint16) bool  { return true }
func offline(uint16) bool { return false }

func TestTimeout(t *testing.T) {
	want := []time.Duration{5, 10, 20, 40, 60, 60, 60}

	for failures, w := range want {
		if got := Timeout(failures); got != w*time.Second {
			t.Errorf("falhas=%d: %v, esperado %v", failures, got, w*time.Second)
		}
	}
}

func TestAddCriaIDsDiferentes(t *testing.T) {
	m := New()
	ds := m.Add(1, []uint16{10, 20})

	if len(ds) != 2 {
		t.Fatalf("esperadas 2 entregas, recebidas %d", len(ds))
	}
	if ds[0].ID == 0 || ds[1].ID == 0 || ds[0].ID == ds[1].ID {
		t.Fatalf("IDs inválidos: %d e %d", ds[0].ID, ds[1].ID)
	}
	if m.Pending(1) != 2 {
		t.Fatalf("Pending = %d, esperado 2", m.Pending(1))
	}
}

func TestAck(t *testing.T) {
	m := New()
	ds := m.Add(1, []uint16{10, 20})

	if _, ok := m.Ack(ds[0].ID, 99); ok {
		t.Fatal("ACK de outro cliente foi aceito")
	}
	if ref, ok := m.Ack(ds[0].ID, 10); !ok || ref != 1 {
		t.Fatalf("Ack: ref=%d ok=%v", ref, ok)
	}
	if _, ok := m.Ack(ds[0].ID, 10); ok {
		t.Fatal("ACK repetido foi aceito")
	}
	if m.Pending(1) != 1 {
		t.Fatalf("Pending = %d, esperado 1", m.Pending(1))
	}

	m.Ack(ds[1].ID, 20)
	if m.Pending(1) != 0 {
		t.Fatal("publicação deveria ter terminado")
	}
}

// DELIVERY_ACK perdido: reenvio com o mesmo ID depois do prazo.
func TestReenvioAposPrazo(t *testing.T) {
	m := New()
	d := m.Add(1, []uint16{10})[0]

	send, _ := m.Tick(t0, online)
	if len(send) != 1 {
		t.Fatalf("deveria enviar 1, enviou %d", len(send))
	}
	m.MarkSent(d.ID, t0)

	if send, _ := m.Tick(t0.Add(4*time.Second), online); len(send) != 0 {
		t.Fatal("reenviou antes dos 5 s")
	}

	send, _ = m.Tick(t0.Add(5*time.Second), online)
	if len(send) != 1 || send[0].ID != d.ID || send[0].Failures != 1 {
		t.Fatalf("reenvio errado: %+v", send)
	}

	// O segundo prazo é de 10 s.
	m.MarkSent(d.ID, t0.Add(5*time.Second))
	if send, _ := m.Tick(t0.Add(14*time.Second), online); len(send) != 0 {
		t.Fatal("reenviou antes dos 10 s")
	}
	if send, _ := m.Tick(t0.Add(15*time.Second), online); len(send) != 1 {
		t.Fatal("não reenviou depois dos 10 s")
	}
}

func TestQuarentenaNaDecimaFalha(t *testing.T) {
	m := New()
	d := m.Add(1, []uint16{10})[0]
	now := t0

	for i := 1; i <= MaxAttempts; i++ {
		m.MarkSent(d.ID, now)
		now = now.Add(Timeout(i - 1))
		_, q := m.Tick(now, online)

		if i < MaxAttempts && len(q) != 0 {
			t.Fatalf("quarentena antes da hora, na falha %d", i)
		}
		if i == MaxAttempts && (len(q) != 1 || q[0].ID != d.ID) {
			t.Fatalf("esperada quarentena na 10ª falha: %+v", q)
		}
	}

	if m.Pending(1) != 0 {
		t.Fatal("entrega em quarentena continua pendente")
	}
	if _, ok := m.Ack(d.ID, 10); ok {
		t.Fatal("ACK de entrega em quarentena foi aceito")
	}
}

// Cliente offline não recebe nem gasta tentativas.
func TestClienteOffline(t *testing.T) {
	m := New()
	d := m.Add(1, []uint16{10})[0]

	for i := 0; i < 50; i++ {
		send, q := m.Tick(t0.Add(time.Duration(i)*time.Minute), offline)
		if len(send)+len(q) != 0 {
			t.Fatal("offline não deve enviar nem ir para a quarentena")
		}
	}

	// Caiu com a entrega em voo: volta a aguardar, sem falha.
	m.MarkSent(d.ID, t0)
	m.Disconnected(10)
	if _, q := m.Tick(t0.Add(time.Hour), offline); len(q) != 0 {
		t.Fatal("a queda contou como falha")
	}

	send, _ := m.Tick(t0.Add(time.Hour), online)
	if len(send) != 1 || send[0].Failures != 0 {
		t.Fatalf("ao reconectar: %+v", send)
	}
}

func TestDisconnectedNaoMexeEmOutrosClientes(t *testing.T) {
	m := New()
	ds := m.Add(1, []uint16{10, 20})
	m.MarkSent(ds[0].ID, t0)
	m.MarkSent(ds[1].ID, t0)

	m.Disconnected(10)

	// Só a do 10 volta a aguardar; a do 20 continua em voo até o prazo.
	send, _ := m.Tick(t0.Add(time.Second), online)
	if len(send) != 1 || send[0].Client != 10 {
		t.Fatalf("esperado reenviar só a do cliente 10: %+v", send)
	}
}

func TestLookup(t *testing.T) {
	m := New()
	d := m.Add(5, []uint16{10})[0]

	got, ok := m.Lookup(d.ID)
	if !ok || got.Ref != 5 || got.Client != 10 {
		t.Fatalf("Lookup: %+v %v", got, ok)
	}
	m.Ack(d.ID, 10)
	if _, ok := m.Lookup(d.ID); ok {
		t.Fatal("entrega confirmada não deveria ser encontrada")
	}
}

// A janela por assinante do servidor depende destas contagens.
func TestInFlightEWaiting(t *testing.T) {
	m := New()
	now := time.Unix(0, 0)
	ds := m.Add(1, []uint16{10, 20})
	more := m.Add(2, []uint16{10})

	if n := len(m.Waiting(10)); n != 2 {
		t.Fatalf("Waiting(10) = %d, esperado 2", n)
	}
	m.MarkSent(ds[0].ID, now)
	m.MarkSent(ds[0].ID, now) // marcar de novo não conta duas vezes
	if m.InFlight(10) != 1 || len(m.Waiting(10)) != 1 || m.Waiting(10)[0].ID != more[0].ID {
		t.Fatalf("depois do envio: InFlight=%d Waiting=%v", m.InFlight(10), m.Waiting(10))
	}
	if m.InFlight(20) != 0 {
		t.Fatalf("InFlight(20) = %d, esperado 0", m.InFlight(20))
	}

	// Prazo vencido: volta a aguardar e sai da contagem em voo.
	m.Tick(now.Add(InitialTimeout), func(uint16) bool { return true })
	if m.InFlight(10) != 0 || len(m.Waiting(10)) != 2 {
		t.Fatalf("depois do prazo: InFlight=%d Waiting=%d", m.InFlight(10), len(m.Waiting(10)))
	}

	// Desconexão zera a contagem; ACK tira a entrega dos índices.
	m.MarkSent(ds[0].ID, now)
	m.MarkSent(more[0].ID, now)
	m.Disconnected(10)
	if m.InFlight(10) != 0 || len(m.Waiting(10)) != 2 {
		t.Fatalf("depois da desconexão: InFlight=%d Waiting=%d", m.InFlight(10), len(m.Waiting(10)))
	}
	m.MarkSent(ds[0].ID, now)
	if _, ok := m.Ack(ds[0].ID, 10); !ok {
		t.Fatal("Ack falhou")
	}
	if m.InFlight(10) != 0 || len(m.Waiting(10)) != 1 {
		t.Fatalf("depois do ACK: InFlight=%d Waiting=%d", m.InFlight(10), len(m.Waiting(10)))
	}

	// Quarentena também tira dos índices.
	m.MarkSent(more[0].ID, now)
	for i := 0; i < MaxAttempts; i++ {
		now = now.Add(MaxBackoff)
		m.Tick(now, func(uint16) bool { return false })
		m.MarkSent(more[0].ID, now)
	}
	if m.InFlight(10) != 0 || len(m.Waiting(10)) != 0 {
		t.Fatalf("depois da quarentena: InFlight=%d Waiting=%d", m.InFlight(10), len(m.Waiting(10)))
	}
}
