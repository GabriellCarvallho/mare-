package walstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/wal"
)

func TestConvErr(t *testing.T) {
	if convErr(nil) != nil {
		t.Fatal("nil deveria continuar nil")
	}
	if err := convErr(wal.ErrStorageFull); !errors.Is(err, server.ErrStorageFull) {
		t.Fatalf("ErrStorageFull virou %v", err)
	}
	if err := convErr(wal.ErrBroken); !errors.Is(err, server.ErrLogBroken) {
		t.Fatalf("ErrBroken virou %v", err)
	}
	outro := errors.New("outro")
	if convErr(outro) != outro {
		t.Fatal("erro desconhecido deveria passar sem mudança")
	}
}

// Grava, fecha, reabre e relê: os registros e os seq voltam iguais.
func TestGravaEReabre(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log-1.wal")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Replay(func(uint64, server.Record) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := l.Append(server.Record{Kind: 1, Data: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var got []byte
	if err := l.Replay(func(_ uint64, r server.Record) error { got = append(got, r.Data...); return nil }); err != nil {
		t.Fatal(err)
	}
	if string(got) != "\x01\x02\x03" {
		t.Fatalf("relido % X", got)
	}
}

// Disco cheio vira server.ErrStorageFull (o broker responde STORAGE_UNAVAILABLE).
func TestDiscoCheio(t *testing.T) {
	l, err := OpenLimit(filepath.Join(t.TempDir(), "log-1.wal"), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Replay(func(uint64, server.Record) error { return nil })
	for i := 0; i < 100; i++ {
		if _, err := l.Append(server.Record{Kind: 1, Data: make([]byte, 16)}); err != nil {
			if !errors.Is(err, server.ErrStorageFull) {
				t.Fatalf("esperado ErrStorageFull, obtido %v", err)
			}
			return
		}
	}
	t.Fatal("o log deveria ter enchido")
}
