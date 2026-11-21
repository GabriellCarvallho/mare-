// Package sched é o escalonador de publicações do broker (documento de
// projeto, Seção 7).
//
// Duas classes, CRÍTICA e NORMAL. Quando as duas têm itens, saem até 3
// críticos para cada normal. Dentro de cada classe há rodízio entre
// publicadores: cada um tem a sua própria fila FIFO, e os publicadores são
// atendidos em círculo, um item por vez. Assim um sensor com fluxo contínuo
// não atrasa os de baixa taxa.
//
// Cotas: cada publicador pode ter até 1.000 itens não concluídos (na fila ou
// com entregas em andamento), e a fila global tem um limite de itens
// aguardando. Acima disso, Enqueue devolve ErrOverloaded.
package sched

import (
	"errors"
	"sync"
)

var ErrOverloaded = errors.New("cota excedida")

// ErrDuplicateRef é uma extensão do contrato: o mesmo Ref enfileirado duas
// vezes antes do Done indica erro de quem chama.
var ErrDuplicateRef = errors.New("referência já enfileirada")

type Item struct {
	Publisher uint16
	Critical  bool
	Ref       uint64 // referência da publicação no broker
}

// Todos os métodos podem ser chamados por várias goroutines ao mesmo
// tempo: a sincronização é interna.
type Scheduler interface {
	// Não bloqueia. ErrOverloaded quando o publicador já tem 1.000
	// itens não concluídos ou a fila global está cheia.
	Enqueue(it Item) error

	// Não bloqueia: devolve false se não houver item. Para esperar,
	// use Ready, que recebe um sinal a cada Enqueue.
	Next() (Item, bool)
	Ready() <-chan struct{}

	// Libera a cota do publicador quando todas as entregas do item
	// terminam (confirmadas ou em quarentena).
	Done(it Item)
}

// Config traz os limites da tabela 7.1. Valores zero usam o padrão.
type Config struct {
	PerPublisher  int // itens não concluídos por publicador (padrão 1.000)
	MaxQueued     int // itens aguardando na fila global (padrão 100.000)
	CriticalBurst int // críticos por normal quando há os dois (padrão 3)
}

const (
	DefaultPerPublisher  = 1000
	DefaultMaxQueued     = 100_000
	DefaultCriticalBurst = 3
)

// New cria um escalonador vazio.
func New(cfg Config) Scheduler {
	if cfg.PerPublisher <= 0 {
		cfg.PerPublisher = DefaultPerPublisher
	}
	if cfg.MaxQueued <= 0 {
		cfg.MaxQueued = DefaultMaxQueued
	}
	if cfg.CriticalBurst <= 0 {
		cfg.CriticalBurst = DefaultCriticalBurst
	}
	return &scheduler{
		cfg:         cfg,
		critical:    newClass(),
		normal:      newClass(),
		outstanding: make(map[uint64]uint16),
		perPub:      make(map[uint16]int),
		ready:       make(chan struct{}, 1),
	}
}

type scheduler struct {
	mu  sync.Mutex
	cfg Config

	critical, normal *class
	streak           int // críticos seguidos desde o último normal

	// Itens não concluídos (na fila ou já entregues ao broker, sem Done).
	outstanding map[uint64]uint16 // Ref -> publicador
	perPub      map[uint16]int    // publicador -> itens não concluídos

	ready chan struct{} // buffer 1: um sinal pendente basta
}

func (s *scheduler) Enqueue(it Item) error {
	s.mu.Lock()
	if _, dup := s.outstanding[it.Ref]; dup {
		s.mu.Unlock()
		return ErrDuplicateRef
	}
	if s.perPub[it.Publisher] >= s.cfg.PerPublisher ||
		s.critical.size+s.normal.size >= s.cfg.MaxQueued {
		s.mu.Unlock()
		return ErrOverloaded
	}
	s.outstanding[it.Ref] = it.Publisher
	s.perPub[it.Publisher]++
	if it.Critical {
		s.critical.push(it)
	} else {
		s.normal.push(it)
	}
	s.mu.Unlock()

	// Sinal sem bloquear: se já houver um pendente, quem espera vai acordar
	// de qualquer jeito e chamar Next até esvaziar.
	select {
	case s.ready <- struct{}{}:
	default:
	}
	return nil
}

func (s *scheduler) Next() (Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hasCrit, hasNorm := s.critical.size > 0, s.normal.size > 0
	switch {
	case hasCrit && (!hasNorm || s.streak < s.cfg.CriticalBurst):
		s.streak++
		return s.critical.pop(), true
	case hasNorm:
		s.streak = 0
		return s.normal.pop(), true
	}
	return Item{}, false
}

func (s *scheduler) Ready() <-chan struct{} { return s.ready }

func (s *scheduler) Done(it Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pub, ok := s.outstanding[it.Ref]
	if !ok {
		return // idempotente: Done repetido ou de item desconhecido
	}
	delete(s.outstanding, it.Ref)
	if s.perPub[pub]--; s.perPub[pub] == 0 {
		delete(s.perPub, pub)
	}
}

// ---------------------------------------------------------------------------
// class: filas por publicador com rodízio

type class struct {
	queues map[uint16][]Item // fila FIFO de cada publicador
	ring   []uint16          // publicadores com itens, na ordem de atendimento
	size   int
}

func newClass() *class { return &class{queues: make(map[uint16][]Item)} }

func (c *class) push(it Item) {
	q, active := c.queues[it.Publisher]
	if !active {
		c.ring = append(c.ring, it.Publisher) // entra no fim do rodízio
	}
	c.queues[it.Publisher] = append(q, it)
	c.size++
}

// pop atende o publicador da vez; se ele ainda tiver itens, volta para o fim.
// Deve ser chamado só com size > 0.
func (c *class) pop() Item {
	pub := c.ring[0]
	c.ring = c.ring[1:]
	q := c.queues[pub]
	it := q[0]
	q[0] = Item{} // solta a referência para o coletor de lixo
	q = q[1:]
	if len(q) == 0 {
		delete(c.queues, pub)
	} else {
		c.queues[pub] = q
		c.ring = append(c.ring, pub)
	}
	c.size--
	return it
}
