package sched

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func enqueue(t *testing.T, s Scheduler, it Item) {
	t.Helper()
	if err := s.Enqueue(it); err != nil {
		t.Fatalf("Enqueue(%+v): %v", it, err)
	}
}

func drain(s Scheduler) []Item {
	var out []Item
	for {
		it, ok := s.Next()
		if !ok {
			return out
		}
		out = append(out, it)
	}
}

func TestEmpty(t *testing.T) {
	if _, ok := New(Config{}).Next(); ok {
		t.Fatal("escalonador vazio devolveu item")
	}
}

// Com as duas filas cheias: C C C N C C C N ...
func TestRatio3To1(t *testing.T) {
	s := New(Config{})
	ref := uint64(0)
	for i := 0; i < 30; i++ {
		ref++
		enqueue(t, s, Item{Publisher: 1, Critical: true, Ref: ref})
		ref++
		enqueue(t, s, Item{Publisher: 2, Critical: false, Ref: ref})
	}
	got := drain(s)
	// Enquanto as duas classes têm itens (primeiros 40 = 30 C + 10 N),
	// cada bloco de 4 é C C C N.
	for i := 0; i < 40; i++ {
		wantCrit := i%4 != 3
		if got[i].Critical != wantCrit {
			t.Fatalf("posição %d: crítico=%v, esperado %v", i, got[i].Critical, wantCrit)
		}
	}
	// Críticos acabaram: o resto são os 20 normais.
	for i := 40; i < 60; i++ {
		if got[i].Critical {
			t.Fatalf("posição %d deveria ser normal", i)
		}
	}
	if len(got) != 60 {
		t.Fatalf("%d itens, esperado 60", len(got))
	}
}

// Sem normais, os críticos não esperam; sem críticos, os normais seguem.
func TestOnlyOneClass(t *testing.T) {
	s := New(Config{})
	for i := uint64(1); i <= 10; i++ {
		enqueue(t, s, Item{Publisher: 1, Critical: true, Ref: i})
	}
	if n := len(drain(s)); n != 10 {
		t.Fatalf("críticos: %d", n)
	}
	for i := uint64(11); i <= 20; i++ {
		enqueue(t, s, Item{Publisher: 1, Ref: i})
	}
	if n := len(drain(s)); n != 10 {
		t.Fatalf("normais: %d", n)
	}
}

// Normal chega depois de uma rajada de críticos: não espera além de 3.
func TestNormalNotStarved(t *testing.T) {
	s := New(Config{})
	for i := uint64(1); i <= 10; i++ {
		enqueue(t, s, Item{Publisher: 1, Critical: true, Ref: i})
	}
	for i := 0; i < 5; i++ { // a sequência de críticos já está em 5
		s.Next()
	}
	enqueue(t, s, Item{Publisher: 2, Ref: 100})
	if it, _ := s.Next(); it.Ref != 100 {
		t.Fatalf("normal deveria sair já, saiu %+v", it)
	}
}

// Rodízio dentro da classe: A tem 5 itens, B e C têm 1; a ordem é A B C A A A A.
func TestRoundRobinWithinClass(t *testing.T) {
	s := New(Config{})
	for i := uint64(1); i <= 5; i++ {
		enqueue(t, s, Item{Publisher: 'A', Ref: i})
	}
	enqueue(t, s, Item{Publisher: 'B', Ref: 10})
	enqueue(t, s, Item{Publisher: 'C', Ref: 20})

	var order []uint16
	for _, it := range drain(s) {
		order = append(order, it.Publisher)
	}
	want := []uint16{'A', 'B', 'C', 'A', 'A', 'A', 'A'}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("ordem %q, esperado %q", order, want)
		}
	}
	// FIFO dentro do mesmo publicador.
	s2 := New(Config{})
	for i := uint64(1); i <= 3; i++ {
		enqueue(t, s2, Item{Publisher: 1, Ref: i})
	}
	for i, it := range drain(s2) {
		if it.Ref != uint64(i+1) {
			t.Fatalf("FIFO quebrado: %+v", it)
		}
	}
}

// Sensor que publica sem parar recebe OVERLOADED sem atrasar os outros.
func TestFloodingPublisher(t *testing.T) {
	s := New(Config{})
	var ref uint64
	var refused int
	for i := 0; i < 1500; i++ {
		ref++
		if err := s.Enqueue(Item{Publisher: 1, Ref: ref}); errors.Is(err, ErrOverloaded) {
			refused++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if refused != 500 {
		t.Fatalf("recusados %d, esperado 500 (cota de 1.000)", refused)
	}
	// Outro sensor ainda é aceito e sai logo no segundo Next, não depois dos 1.000.
	enqueue(t, s, Item{Publisher: 2, Ref: 99999})
	s.Next()
	if it, _ := s.Next(); it.Publisher != 2 {
		t.Fatalf("sensor 2 deveria ser atendido no rodízio, saiu %+v", it)
	}
}

// A cota conta itens não concluídos: sair da fila (Next) não libera; Done libera.
func TestQuotaReleasedByDone(t *testing.T) {
	s := New(Config{PerPublisher: 2})
	a, b, c := Item{Publisher: 1, Ref: 1}, Item{Publisher: 1, Ref: 2}, Item{Publisher: 1, Ref: 3}
	enqueue(t, s, a)
	enqueue(t, s, b)
	drain(s)
	if err := s.Enqueue(c); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("itens em entrega ainda contam na cota: %v", err)
	}
	s.Done(a)
	s.Done(a) // idempotente: não libera duas vezes
	enqueue(t, s, c)
	if err := s.Enqueue(Item{Publisher: 1, Ref: 4}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("Done repetido liberou cota extra: %v", err)
	}
}

func TestGlobalLimit(t *testing.T) {
	s := New(Config{MaxQueued: 3})
	for i := uint64(1); i <= 3; i++ {
		enqueue(t, s, Item{Publisher: uint16(i), Ref: i})
	}
	if err := s.Enqueue(Item{Publisher: 9, Ref: 9}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("fila global cheia: %v", err)
	}
	s.Next() // sair da fila libera espaço global
	enqueue(t, s, Item{Publisher: 9, Ref: 9})
}

func TestDuplicateRef(t *testing.T) {
	s := New(Config{})
	it := Item{Publisher: 1, Ref: 5}
	enqueue(t, s, it)
	if err := s.Enqueue(it); !errors.Is(err, ErrDuplicateRef) {
		t.Fatalf("esperado ErrDuplicateRef, obtido %v", err)
	}
}

func TestReadySignal(t *testing.T) {
	s := New(Config{})
	select {
	case <-s.Ready():
		t.Fatal("sinal sem Enqueue")
	default:
	}
	enqueue(t, s, Item{Publisher: 1, Ref: 1})
	enqueue(t, s, Item{Publisher: 1, Ref: 2}) // não bloqueia com sinal pendente
	select {
	case <-s.Ready():
	case <-time.After(time.Second):
		t.Fatal("Ready não sinalizou")
	}
}

// Produtores e um consumidor concorrentes: nada se perde nem duplica.
func TestConcurrentProducersConsumer(t *testing.T) {
	s := New(Config{})
	const producers, perProducer = 8, 500
	total := producers * perProducer

	seen := make(map[uint64]bool)
	done := make(chan struct{})
	go func() { // consumidor no padrão que o broker vai usar
		defer close(done)
		for len(seen) < total {
			it, ok := s.Next()
			if !ok {
				<-s.Ready()
				continue
			}
			if seen[it.Ref] {
				t.Errorf("item duplicado %d", it.Ref)
			}
			seen[it.Ref] = true
			s.Done(it)
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				ref := uint64(p*perProducer + i)
				for s.Enqueue(Item{Publisher: uint16(p), Critical: i%2 == 0, Ref: ref}) != nil {
					time.Sleep(time.Millisecond) // OVERLOADED: tenta de novo
				}
			}
		}(p)
	}
	wg.Wait()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("consumidor parou com %d de %d itens", len(seen), total)
	}
}
