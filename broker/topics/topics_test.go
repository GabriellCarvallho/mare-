package topics

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

func mustCreate(t *testing.T, r Registry, name string) uint8 {
	t.Helper()
	id, err := r.Create(name)
	if err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
	return id
}

func TestCreateIdempotentAndResolve(t *testing.T) {
	r := New()
	a := mustCreate(t, r, "mar/temp")
	b := mustCreate(t, r, "mar/sal")
	if a != 1 || b != 2 {
		t.Fatalf("ids %d e %d, esperado 1 e 2", a, b)
	}
	if again := mustCreate(t, r, "mar/temp"); again != a {
		t.Fatalf("Create repetido devolveu %d, esperado %d", again, a)
	}
	if id, err := r.Resolve("mar/sal"); err != nil || id != b {
		t.Fatalf("Resolve: %d %v", id, err)
	}
	if _, err := r.Resolve("nao/existe"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("esperado ErrTopicNotFound, obtido %v", err)
	}
}

func TestLimit255(t *testing.T) {
	r := New()
	for i := 1; i <= MaxTopics; i++ {
		if id := mustCreate(t, r, fmt.Sprintf("t%d", i)); int(id) != i {
			t.Fatalf("tópico %d recebeu id %d", i, id)
		}
	}
	if _, err := r.Create("t256"); !errors.Is(err, ErrTooManyTopics) {
		t.Fatalf("esperado ErrTooManyTopics, obtido %v", err)
	}
	// Um nome já existente continua idempotente mesmo com o registro cheio.
	if id, err := r.Create("t10"); err != nil || id != 10 {
		t.Fatalf("Create idempotente com registro cheio: %d %v", id, err)
	}
}

func TestDeleteWithPendingKeepsID(t *testing.T) {
	r := New()
	id := mustCreate(t, r, "mar/temp")
	if err := r.Subscribe(id, 100); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(id); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(id); err != nil {
		t.Fatalf("Delete deve ser idempotente: %v", err)
	}

	// EM_REMOÇÃO: recusa publicações (Snapshot), assinaturas e recriação.
	if _, err := r.Snapshot(id); !errors.Is(err, ErrTopicRemoving) {
		t.Errorf("Snapshot: esperado ErrTopicRemoving, obtido %v", err)
	}
	if err := r.Subscribe(id, 200); !errors.Is(err, ErrTopicRemoving) {
		t.Errorf("Subscribe: esperado ErrTopicRemoving, obtido %v", err)
	}
	if _, err := r.Create("mar/temp"); !errors.Is(err, ErrTopicRemoving) {
		t.Errorf("Create: esperado ErrTopicRemoving, obtido %v", err)
	}
	// O id continua ocupado enquanto houver pendências: outro tópico não o recebe.
	if other := mustCreate(t, r, "mar/sal"); other == id {
		t.Fatalf("topic_id %d reutilizado antes do Purge", id)
	}

	// Purge libera o id e o nome.
	if err := r.Purge(id); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve("mar/temp"); !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Resolve após Purge: %v", err)
	}
	if reused := mustCreate(t, r, "mar/vento"); reused != id {
		t.Errorf("após Purge, esperado reuso do id %d, obtido %d", id, reused)
	}
}

func TestPurgeErrors(t *testing.T) {
	r := New()
	id := mustCreate(t, r, "a")
	if err := r.Purge(id); !errors.Is(err, ErrNotRemoving) {
		t.Errorf("Purge de tópico ativo: %v", err)
	}
	if err := r.Purge(99); !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Purge inexistente: %v", err)
	}
	if err := r.Delete(99); !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Delete inexistente: %v", err)
	}
}

func TestSubscriptionsAndSnapshot(t *testing.T) {
	r := New()
	id := mustCreate(t, r, "mar/temp")

	if _, err := r.Snapshot(id); !errors.Is(err, ErrNoSubscribers) {
		t.Fatalf("esperado ErrNoSubscribers, obtido %v", err)
	}
	for _, c := range []uint16{30, 10, 20, 10} { // 10 repetido: idempotente
		if err := r.Subscribe(id, c); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := r.Snapshot(id)
	if err != nil || !slices.Equal(snap, []uint16{10, 20, 30}) {
		t.Fatalf("Snapshot: %v %v", snap, err)
	}

	// O snapshot é uma cópia: mudanças posteriores não o afetam.
	snap[0] = 999
	if err := r.Unsubscribe(id, 20); err != nil {
		t.Fatal(err)
	}
	if err := r.Unsubscribe(id, 20); err != nil {
		t.Fatalf("Unsubscribe deve ser idempotente: %v", err)
	}
	if again, _ := r.Snapshot(id); !slices.Equal(again, []uint16{10, 30}) {
		t.Fatalf("Snapshot após Unsubscribe: %v", again)
	}

	for _, err := range []error{r.Subscribe(77, 1), r.Unsubscribe(77, 1)} {
		if !errors.Is(err, ErrTopicNotFound) {
			t.Errorf("tópico inexistente: %v", err)
		}
	}
	if _, err := r.Snapshot(77); !errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Snapshot inexistente: %v", err)
	}
}

func TestRestore(t *testing.T) {
	r := New()
	if err := r.Restore(7, "mar/temp"); err != nil {
		t.Fatal(err)
	}
	if err := r.Restore(7, "mar/temp"); err != nil {
		t.Fatalf("Restore idempotente: %v", err)
	}
	if id, _ := r.Resolve("mar/temp"); id != 7 {
		t.Fatalf("Resolve após Restore: %d", id)
	}
	if err := r.Restore(7, "outro"); !errors.Is(err, ErrIDInUse) {
		t.Errorf("id ocupado: %v", err)
	}
	if err := r.Restore(8, "mar/temp"); !errors.Is(err, ErrIDInUse) {
		t.Errorf("nome com outro id: %v", err)
	}
	// Create não pisa em ids restaurados.
	if id := mustCreate(t, r, "novo"); id != 1 {
		t.Errorf("Create após Restore: %d", id)
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id, err := r.Create(fmt.Sprintf("t%d", i%20))
				if err != nil {
					continue
				}
				_ = r.Subscribe(id, uint16(g))
				_, _ = r.Snapshot(id)
				_, _ = r.Resolve(fmt.Sprintf("t%d", i%20))
				_ = r.Unsubscribe(id, uint16(g))
			}
		}(g)
	}
	wg.Wait()
	// Todos os goroutines criaram os mesmos 20 nomes: ids 1..20.
	for i := 0; i < 20; i++ {
		if id, err := r.Resolve(fmt.Sprintf("t%d", i)); err != nil || id < 1 || id > 20 {
			t.Fatalf("t%d: %d %v", i, id, err)
		}
	}
}
