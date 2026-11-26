// Package walstore liga o wal do Felipe (broker/wal) ao broker (server.Log).
//
// Os dois pacotes têm tipos parecidos mas diferentes (wal.Record e
// server.Record) e erros próprios. O adaptador só converte de um para o
// outro; nenhum dos dois pacotes precisa conhecer o outro.
//
//	server.Record  ⇄  wal.Record        (mesmos campos: Kind e Data)
//	wal.ErrStorageFull → server.ErrStorageFull  (broker responde STORAGE_UNAVAILABLE)
//	wal.ErrBroken      → server.ErrLogBroken    (broker encerra: disco não confiável)
//
// Usado pelo cmd/broker (produção), pelos testes de ponta a ponta e pelo
// cmd/marebench.
package walstore

import (
	"errors"
	"fmt"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/wal"
)

// MaxBytes é o limite do log no disco (Seção 7.1: 512 MiB).
const MaxBytes = 512 << 20

// Open abre o log em disco com o limite padrão. É o server.Config.OpenLog
// de produção.
func Open(path string) (server.Log, error) {
	return OpenLimit(path, MaxBytes)
}

// OpenLimit abre o log com outro limite (testes de disco cheio).
func OpenLimit(path string, maxBytes int64) (server.Log, error) {
	l, err := wal.Open(path, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("abrir log %s: %w", path, convErr(err))
	}
	return adapter{l}, nil
}

type adapter struct{ l wal.Log }

func (a adapter) Append(r server.Record) (uint64, error) {
	seq, err := a.l.Append(wal.Record{Kind: r.Kind, Data: r.Data})
	return seq, convErr(err)
}

func (a adapter) Replay(fn func(seq uint64, r server.Record) error) error {
	return convErr(a.l.Replay(func(seq uint64, r wal.Record) error {
		return fn(seq, server.Record{Kind: r.Kind, Data: r.Data})
	}))
}

func (a adapter) Compact(keep func(seq uint64, r server.Record) bool) error {
	return convErr(a.l.Compact(func(seq uint64, r wal.Record) bool {
		return keep(seq, server.Record{Kind: r.Kind, Data: r.Data})
	}))
}

func (a adapter) Close() error { return convErr(a.l.Close()) }

// convErr traduz os erros do wal para os que o broker entende. O erro
// original continua dentro, para aparecer nas mensagens de log.
func convErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, wal.ErrStorageFull):
		return fmt.Errorf("%w (%v)", server.ErrStorageFull, err)
	case errors.Is(err, wal.ErrBroken):
		return fmt.Errorf("%w (%v)", server.ErrLogBroken, err)
	}
	return err
}
