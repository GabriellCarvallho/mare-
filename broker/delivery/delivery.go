// Package delivery acompanha cada entrega de uma publicação a um assinante
// (Seções 4.1 e 4.3 do documento de projeto).
//
//	AGUARDANDO ──envia──► EM_VOO ──DELIVERY_ACK──► confirmada (sai do mapa)
//	     ▲                   │
//	     └── prazo vence ────┘  falha +1; prazos: 5s, 10s, 20s, 40s, 60s...
//	                         │
//	                         └── 10ª falha ──► QUARENTENA (sai do mapa)
//
// O pacote não usa rede, goroutines nem relógio próprio: quem chama informa
// a hora (now). Também não é seguro para uso concorrente: o servidor deve
// chamá-lo sempre com o próprio mutex travado.
package delivery

import (
	"sort"
	"time"
)

const (
	InitialTimeout = 5 * time.Second
	MaxBackoff     = 60 * time.Second
	MaxAttempts    = 10
)

type state uint8

const (
	waiting  state = iota // aguardando envio
	inFlight              // enviada, esperando ACK
)

// Delivery é uma entrega: uma publicação (Ref) para um assinante (Client).
type Delivery struct {
	ID       uint32
	Ref      uint64
	Client   uint16
	Failures int
}

type entry struct {
	Delivery
	state    state
	deadline time.Time
}

type Manager struct {
	nextID uint32
	byID   map[uint32]*entry // só entregas pendentes (aguardando ou em voo)
}

func New() *Manager {
	return &Manager{byID: make(map[uint32]*entry)}
}

// Timeout devolve o prazo depois de n falhas: 5s, 10s, 20s, 40s, 60s, 60s...
func Timeout(failures int) time.Duration {
	t := InitialTimeout

	for i := 0; i < failures && t < MaxBackoff; i++ {
		t *= 2
	}

	return min(t, MaxBackoff)
}

// newID devolve o próximo ID livre. Pula o 0 e IDs ainda pendentes (caso o
// contador dê a volta depois de 4 bilhões de entregas).
func (m *Manager) newID() uint32 {
	for {
		m.nextID++
		if m.nextID != 0 && m.byID[m.nextID] == nil {
			return m.nextID
		}
	}
}

// Add cria uma entrega aguardando para cada cliente.
func (m *Manager) Add(ref uint64, clients []uint16) []Delivery {
	deliveries := make([]Delivery, 0, len(clients))

	for _, client := range clients {
		d := Delivery{
			ID:     m.newID(),
			Ref:    ref,
			Client: client,
		}

		m.byID[d.ID] = &entry{
			Delivery: d,
			state:    waiting,
		}

		deliveries = append(deliveries, d)
	}

	return deliveries
}

// MarkSent registra o envio e define o prazo para o ACK.
func (m *Manager) MarkSent(id uint32, now time.Time) {
	e, ok := m.byID[id]
	if !ok {
		return
	}

	e.state = inFlight
	e.deadline = now.Add(Timeout(e.Failures))
}

// Lookup devolve uma entrega ainda pendente pelo ID (usado pelo servidor
// para gravar o DELIVERED no log antes de confirmar).
func (m *Manager) Lookup(id uint32) (Delivery, bool) {
	e, ok := m.byID[id]
	if !ok {
		return Delivery{}, false
	}
	return e.Delivery, true
}

// Ack confirma a entrega se o ID e o cliente forem correspondentes.
// ACK repetido, atrasado ou de outro cliente devolve ok=false.
func (m *Manager) Ack(id uint32, client uint16) (ref uint64, ok bool) {
	e, exists := m.byID[id]
	if !exists {
		return 0, false
	}

	if e.Client != client {
		return 0, false
	}

	delete(m.byID, id)

	return e.Ref, true
}

// Disconnected devolve para AGUARDANDO as entregas em voo do cliente, sem
// contar falha: ele recebe de novo quando reconectar.
func (m *Manager) Disconnected(client uint16) {
	for _, e := range m.byID {
		if e.Client == client && e.state == inFlight {
			e.state = waiting
		}
	}
}

// Tick avança o relógio. Entregas em voo com prazo vencido ganham uma falha:
// voltam para AGUARDANDO ou, na 10ª, vão para a quarentena (saem do mapa).
// Devolve as entregas a enviar agora (AGUARDANDO de clientes conectados, em
// ordem de ID) e as que acabaram de entrar em quarentena.
func (m *Manager) Tick(now time.Time, online func(client uint16) bool) (send, quarantined []Delivery) {
	for id, e := range m.byID {
		if e.state == inFlight && !now.Before(e.deadline) {
			e.Failures++

			if e.Failures >= MaxAttempts {
				quarantined = append(quarantined, e.Delivery)
				delete(m.byID, id) // apagar durante o range é permitido em Go
				continue
			}

			e.state = waiting
		}

		if e.state == waiting && online(e.Client) {
			send = append(send, e.Delivery)
		}
	}

	// A ordem do range em map é aleatória: ordenar deixa o resultado estável.
	sort.Slice(send, func(i, j int) bool { return send[i].ID < send[j].ID })
	sort.Slice(quarantined, func(i, j int) bool { return quarantined[i].ID < quarantined[j].ID })

	return send, quarantined
}

// Pending conta as entregas ainda pendentes da publicação. Depois de um Ack
// ou de uma quarentena, Pending(ref) == 0 significa que a publicação terminou.
func (m *Manager) Pending(ref uint64) int {
	n := 0
	for _, e := range m.byID {
		if e.Ref == ref {
			n++
		}
	}
	return n
}
