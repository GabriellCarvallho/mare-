package main

// Adaptador: liga o wal do Felipe (broker/wal) ao broker (server.Log).
//
// Os dois pacotes têm tipos parecidos mas diferentes (wal.Record e
// server.Record) e erros próprios. O adaptador só converte de um para o
// outro; nenhum dos dois pacotes precisa conhecer o outro.
//
//	server.Record  ⇄  wal.Record        (mesmos campos: Kind e Data)
//	wal.ErrStorageFull → server.ErrStorageFull  (broker responde STORAGE_UNAVAILABLE)
//	wal.ErrBroken      → server.ErrLogBroken    (broker encerra: disco não confiável)

import (
	"errors"
	"fmt"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/wal"
)

// maxLogBytes é o limite do log no disco (Seção 7.1: 512 MiB).
const maxLogBytes = 512 << 20

// openWAL é o server.Config.OpenLog de produção.
func openWAL(path string) (server.Log, error) {
	l, err := wal.Open(path, maxLogBytes)
	if err != nil {
		return nil, fmt.Errorf("abrir log %s: %w", path, convErr(err))
	}
	return walAdapter{l}, nil
}

type walAdapter struct{ l wal.Log }

func (a walAdapter) Append(r server.Record) (uint64, error) {
	seq, err := a.l.Append(wal.Record{Kind: r.Kind, Data: r.Data})
	return seq, convErr(err)
}

func (a walAdapter) Replay(fn func(seq uint64, r server.Record) error) error {
	return convErr(a.l.Replay(func(seq uint64, r wal.Record) error {
		return fn(seq, server.Record{Kind: r.Kind, Data: r.Data})
	}))
}

func (a walAdapter) Compact(keep func(seq uint64, r server.Record) bool) error {
	return convErr(a.l.Compact(func(seq uint64, r wal.Record) bool {
		return keep(seq, server.Record{Kind: r.Kind, Data: r.Data})
	}))
}

func (a walAdapter) Close() error { return convErr(a.l.Close()) }

// convErr traduz os erros do wal para os que o broker entende. O erro
// original continua dentro (%w), para aparecer nas mensagens de log.
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
