package server

// Passo 6: a interface do log e um log em memória.
//
// O servidor depende só da interface Log abaixo, que espelha o contrato do
// broker/wal do Felipe. Quando o wal estiver pronto, um adaptador de poucas
// linhas liga um ao outro (ver README). Até lá, os testes e o cmd/broker
// usam o MemoryStore.

import (
	"errors"
	"sync"
)

// Record espelha wal.Record: o wal grava Kind e Data sem interpretar Data.
// Quem define o conteúdo de Data é o pacote broker/logrec (passo 1).
type Record struct {
	Kind byte
	Data []byte
}

// Log espelha a interface wal.Log do contrato da divisão.
type Log interface {
	// Bloqueia até o Sync. Erros: ErrStorageFull (nada é gravado),
	// ErrLogBroken (erro de disco: o broker para de confirmar).
	Append(r Record) (seq uint64, err error)
	// Chamado uma vez, antes do primeiro Append: entrega os registros em ordem.
	Replay(fn func(seq uint64, r Record) error) error
	// Reescreve o log mantendo só os registros em que keep devolve true.
	Compact(keep func(seq uint64, r Record) bool) error
	Close() error
}

// O adaptador do wal traduz wal.ErrStorageFull e wal.ErrBroken para estes,
// para o servidor decidir a resposta com errors.Is.
var (
	ErrStorageFull = errors.New("limite de armazenamento atingido")
	ErrLogBroken   = errors.New("log inutilizado por erro de disco")
	errLogClosed   = errors.New("log fechado")
)

// ---------------------------------------------------------------------------
// Log em memória
//
// Guarda um log por caminho, como o disco guardaria arquivos. Sobrevive ao
// reinício do broker dentro do mesmo processo (os testes de reinício
// dependem disso), mas NÃO a reiniciar o programa.

type MemoryStore struct {
	mu       sync.Mutex
	logs     map[string]*memLog
	maxBytes int64
}

// NewMemoryStore cria um armazenamento vazio. maxBytes <= 0 é ilimitado.
func NewMemoryStore(maxBytes int64) *MemoryStore {
	return &MemoryStore{logs: make(map[string]*memLog), maxBytes: maxBytes}
}

// Open abre (ou reabre) o log do caminho. Serve como Config.OpenLog.
func (s *MemoryStore) Open(path string) (Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l := s.logs[path]
	if l == nil {
		l = &memLog{maxBytes: s.maxBytes}
		s.logs[path] = l
	}
	l.mu.Lock()
	l.closed = false
	l.mu.Unlock()
	return l, nil
}

// Len devolve quantos registros o log do caminho guarda (para testes).
func (s *MemoryStore) Len(path string) int {
	s.mu.Lock()
	l := s.logs[path]
	s.mu.Unlock()
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

type memEntry struct {
	seq uint64
	rec Record
}

type memLog struct {
	mu       sync.Mutex
	entries  []memEntry
	seq      uint64
	size     int64
	maxBytes int64
	closed   bool
}

// recordSize aproxima o tamanho do registro em disco (Data + cabeçalho).
func recordSize(r Record) int64 { return int64(len(r.Data) + 13) }

func (l *memLog) Append(r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, errLogClosed
	}
	if l.maxBytes > 0 && l.size+recordSize(r) > l.maxBytes {
		return 0, ErrStorageFull
	}
	l.seq++
	l.size += recordSize(r)
	data := append([]byte(nil), r.Data...) // cópia: quem chamou pode reutilizar
	l.entries = append(l.entries, memEntry{l.seq, Record{r.Kind, data}})
	return l.seq, nil
}

func (l *memLog) Replay(fn func(uint64, Record) error) error {
	l.mu.Lock()
	snapshot := append([]memEntry(nil), l.entries...)
	l.mu.Unlock()

	for _, e := range snapshot {
		if err := fn(e.seq, e.rec); err != nil {
			return err
		}
	}
	return nil
}

func (l *memLog) Compact(keep func(uint64, Record) bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var kept []memEntry
	l.size = 0
	for _, e := range l.entries {
		if keep(e.seq, e.rec) {
			kept = append(kept, e)
			l.size += recordSize(e.rec)
		}
	}
	l.entries = kept
	return nil
}

func (l *memLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	return nil
}
