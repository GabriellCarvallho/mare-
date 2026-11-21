// Package topics mantém os tópicos e as assinaturas do broker, em memória
// (documento de projeto, Seções 3.5, 4.1 e 5.2).
//
// Ciclo de vida de um tópico:
//
//	Create ──► ATIVO ──Delete──► EM_REMOÇÃO ──Purge──► (removido; topic_id livre)
//
// Em EM_REMOÇÃO o tópico recusa novas publicações e assinaturas, mas o
// topic_id continua ocupado: ele só volta a ser usado depois do Purge, que o
// broker chama quando não há mais entregas pendentes que o referenciem.
package topics

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrTooManyTopics = errors.New("limite de 255 tópicos")
	ErrTopicNotFound = errors.New("tópico inexistente")
	ErrTopicRemoving = errors.New("tópico em remoção")
	ErrNoSubscribers = errors.New("tópico sem assinantes")

	// Extensões do contrato (avisar o Felipe):
	ErrNotRemoving = errors.New("tópico não está em remoção") // Purge de tópico ativo
	ErrIDInUse     = errors.New("topic_id já ocupado")        // Restore em conflito
)

// MaxTopics é o limite imposto pelo campo topic_id de 1 byte (0 é inválido).
const MaxTopics = 255

// Nenhum método bloqueia. Todos podem ser chamados por várias
// goroutines ao mesmo tempo: a sincronização é interna.
type Registry interface {
	// Idempotente: criar um nome que já existe devolve o mesmo topic_id
	// (necessário quando os clientes refazem tópicos após a promoção).
	// Erros: ErrTooManyTopics, ErrTopicRemoving.
	Create(name string) (uint8, error)
	Resolve(name string) (uint8, error) // ErrTopicNotFound

	// Passa para EM_REMOÇÃO: recusa novas publicações e assinaturas.
	Delete(id uint8) error
	// Chamado pelo broker quando o tópico em remoção não tem mais
	// pendências; só então o topic_id pode ser reutilizado.
	Purge(id uint8) error

	// Idempotentes. Erros: ErrTopicNotFound, ErrTopicRemoving.
	Subscribe(id uint8, client uint16) error
	Unsubscribe(id uint8, client uint16) error

	// Cópia da lista de assinantes naquele instante.
	// Erros: ErrTopicNotFound, ErrTopicRemoving, ErrNoSubscribers.
	Snapshot(id uint8) ([]uint16, error)
}

// Restorer é usado pela Parte 3 ao reconstruir o estado a partir do log
// (registro TOPIC_CREATED traz o topic_id original). Não faz parte do
// contrato com o Felipe; New devolve um valor que implementa as duas.
type Restorer interface {
	// Recria um tópico com o topic_id gravado no log.
	// Erros: ErrIDInUse (id ocupado por outro nome), ErrTopicRemoving.
	Restore(id uint8, name string) error
}

type topic struct {
	id       uint8
	name     string
	removing bool
	subs     map[uint16]struct{}
}

type registry struct {
	mu     sync.RWMutex
	byID   [MaxTopics + 1]*topic // índice 0 nunca é usado
	byName map[string]*topic
	count  int
}

// New cria um Registry vazio (o estado inicial de uma reserva promovida).
func New() interface {
	Registry
	Restorer
} {
	return &registry{byName: make(map[string]*topic)}
}

func (r *registry) Create(name string) (uint8, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.byName[name]; ok {
		if t.removing {
			return 0, ErrTopicRemoving
		}
		return t.id, nil // idempotente
	}
	if r.count == MaxTopics {
		return 0, ErrTooManyTopics
	}
	// Menor topic_id livre. Determinístico: reproduzir a mesma sequência de
	// operações gera os mesmos códigos.
	for id := 1; id <= MaxTopics; id++ {
		if r.byID[id] == nil {
			r.add(uint8(id), name)
			return uint8(id), nil
		}
	}
	return 0, ErrTooManyTopics // inalcançável se count estiver correto
}

func (r *registry) Restore(id uint8, name string) error {
	if id == 0 {
		return ErrTopicNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byID[id]; t != nil {
		if t.name != name {
			return ErrIDInUse
		}
		if t.removing {
			return ErrTopicRemoving
		}
		return nil // idempotente
	}
	if t, ok := r.byName[name]; ok && t.id != id {
		return ErrIDInUse
	}
	r.add(id, name)
	return nil
}

func (r *registry) add(id uint8, name string) {
	t := &topic{id: id, name: name, subs: make(map[uint16]struct{})}
	r.byID[id] = t
	r.byName[name] = t
	r.count++
}

func (r *registry) Resolve(name string) (uint8, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.byName[name]
	if !ok {
		return 0, ErrTopicNotFound
	}
	// Um tópico em remoção ainda é resolvido: o cliente recebe
	// TOPIC_REMOVING ao publicar, que é um erro terminal mais informativo.
	return t.id, nil
}

func (r *registry) Delete(id uint8) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.byID[id]
	if t == nil {
		return ErrTopicNotFound
	}
	t.removing = true // idempotente
	return nil
}

func (r *registry) Purge(id uint8) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.byID[id]
	if t == nil {
		return ErrTopicNotFound
	}
	if !t.removing {
		return ErrNotRemoving
	}
	r.byID[id] = nil
	delete(r.byName, t.name)
	r.count--
	return nil
}

// active devolve o tópico se ele existir e não estiver em remoção.
// Deve ser chamado com r.mu travado.
func (r *registry) active(id uint8) (*topic, error) {
	t := r.byID[id]
	if t == nil {
		return nil, ErrTopicNotFound
	}
	if t.removing {
		return nil, ErrTopicRemoving
	}
	return t, nil
}

func (r *registry) Subscribe(id uint8, client uint16) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, err := r.active(id)
	if err != nil {
		return err
	}
	t.subs[client] = struct{}{}
	return nil
}

func (r *registry) Unsubscribe(id uint8, client uint16) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, err := r.active(id)
	if err != nil {
		return err
	}
	// Só impede a inclusão em novos snapshots; entregas já criadas
	// continuam com o gerenciador de entregas (Seção 5.2).
	delete(t.subs, client)
	return nil
}

func (r *registry) Snapshot(id uint8) ([]uint16, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, err := r.active(id)
	if err != nil {
		return nil, err
	}
	if len(t.subs) == 0 {
		return nil, ErrNoSubscribers
	}
	out := make([]uint16, 0, len(t.subs))
	for c := range t.subs {
		out = append(out, c)
	}
	slices.Sort(out) // ordem estável facilita testes e logs
	return out, nil
}
