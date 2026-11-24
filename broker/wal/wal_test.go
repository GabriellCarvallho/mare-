package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTestLog(t *testing.T, max int64) (Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "broker.wal")
	l, err := Open(path, max)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if err := l.Replay(func(uint64, Record) error { return nil }); err != nil {
		t.Fatalf("Replay inicial: %v", err)
	}
	return l, path
}

func replayAll(t *testing.T, l Log) []struct {
	seq uint64
	r   Record
} {
	t.Helper()
	var got []struct {
		seq uint64
		r   Record
	}
	if err := l.Replay(func(seq uint64, r Record) error {
		got = append(got, struct {
			seq uint64
			r   Record
		}{seq, r})
		return nil
	}); err != nil {
		t.Fatalf("Replay(): %v", err)
	}
	return got
}

func TestAppendAndReplay10000(t *testing.T) {
	l, path := openTestLog(t, 0)
	for i := 0; i < 10_000; i++ {
		seq, err := l.Append(Record{Kind: 1, Data: []byte{byte(i), byte(i >> 8)}})
		if err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if seq != uint64(i+1) {
			t.Fatalf("sequência = %d, esperada %d", seq, i+1)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := replayAll(t, l)
	if len(got) != 10_000 {
		t.Fatalf("Replay trouxe %d registros; esperado 10000", len(got))
	}
	if got[0].seq != 1 || got[len(got)-1].seq != 10_000 {
		t.Fatalf("sequências inicial/final incorretas: %d / %d", got[0].seq, got[len(got)-1].seq)
	}
}

func TestReplayTruncatesIncompleteFinalRecord(t *testing.T) {
	l, path := openTestLog(t, 0)
	_, _ = l.Append(Record{Kind: 1, Data: []byte("primeiro")})
	_, _ = l.Append(Record{Kind: 2, Data: []byte("segundo")})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	l, err = Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := replayAll(t, l)
	if len(got) != 1 || got[0].r.Kind != 1 {
		t.Fatalf("registros recuperados = %v; esperado apenas o primeiro", got)
	}
}

func TestReplayTruncatesInvalidChecksum(t *testing.T) {
	l, path := openTestLog(t, 0)
	_, _ = l.Append(Record{Kind: 1, Data: []byte("válido")})
	_, _ = l.Append(Record{Kind: 2, Data: []byte("corromper")})
	_, _ = l.Append(Record{Kind: 3, Data: []byte("depois")})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// O segundo registro começa depois do primeiro quadro.
	firstSize := int64(headerSize + len("válido"))
	var one [1]byte
	if _, err := f.ReadAt(one[:], firstSize+17); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := f.WriteAt(one[:], firstSize+17); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := replayAll(t, l)
	if len(got) != 1 || got[0].r.Kind != 1 {
		t.Fatalf("registros após checksum inválido = %v; esperado apenas o primeiro", got)
	}
}

func TestCompactPreservesSequencesAndNextID(t *testing.T) {
	l, path := openTestLog(t, 0)
	for i := byte(1); i <= 5; i++ {
		if _, err := l.Append(Record{Kind: i, Data: []byte{i}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Compact(func(seq uint64, _ Record) bool { return seq == 2 || seq == 4 }); err != nil {
		t.Fatalf("Compact(): %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := replayAll(t, l)
	if len(got) != 2 || got[0].seq != 2 || got[1].seq != 4 {
		t.Fatalf("registros após compactação = %v; esperado seq 2 e 4", got)
	}
	seq, err := l.Append(Record{Kind: 1, Data: []byte("novo")})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("sequência após compactação = %d; esperado 6", seq)
	}
}

func TestAppendConcurrent(t *testing.T) {
	l, _ := openTestLog(t, 0)
	const workers, each = 20, 100
	var wg sync.WaitGroup
	seqs := make(chan uint64, workers*each)
	errs := make(chan error, workers*each)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; n < each; n++ {
				seq, err := l.Append(Record{Kind: 1, Data: []byte{byte(id), byte(n)}})
				if err != nil {
					errs <- err
					return
				}
				seqs <- seq
			}
		}(worker)
	}
	wg.Wait()
	close(seqs)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make([]bool, workers*each+1)
	for seq := range seqs {
		if seq == 0 || seq >= uint64(len(seen)) || seen[seq] {
			t.Fatalf("sequência inválida ou repetida: %d", seq)
		}
		seen[seq] = true
	}
	for seq := 1; seq < len(seen); seq++ {
		if !seen[seq] {
			t.Fatalf("faltou sequência %d", seq)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageFullDoesNotWrite(t *testing.T) {
	l, path := openTestLog(t, int64(headerSize+4))
	if _, err := l.Append(Record{Kind: 1, Data: []byte("grande")}); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("Append() error = %v; esperado ErrStorageFull", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := replayAll(t, l); len(got) != 0 {
		t.Fatalf("foram gravados %d registros apesar do limite", len(got))
	}
}

func TestReplayCallbackErrorCanBeRetried(t *testing.T) {
	l, path := openTestLog(t, 0)
	if _, err := l.Append(Record{Kind: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	want := errors.New("erro da aplicação")
	if err := l.Replay(func(uint64, Record) error { return want }); !errors.Is(err, want) {
		t.Fatalf("Replay() error = %v; esperado erro do callback", err)
	}
	got := replayAll(t, l)
	if len(got) != 1 {
		t.Fatalf("Replay repetido trouxe %d registros", len(got))
	}
}

func TestCloseRejectsFurtherOperations(t *testing.T) {
	l, _ := openTestLog(t, 0)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(Record{Kind: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append após Close() = %v; esperado ErrClosed", err)
	}
	if err := l.Replay(func(uint64, Record) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Replay após Close() = %v; esperado ErrClosed", err)
	}
}

func TestHeaderUsesBigEndianSequenceAndLength(t *testing.T) {
	b := encodeRecord(3, 0x0102030405060708, []byte("abc"))
	if got := binary.BigEndian.Uint64(b[5:13]); got != 0x0102030405060708 {
		t.Fatalf("sequência no cabeçalho = %#x", got)
	}
	if got := binary.BigEndian.Uint32(b[13:17]); got != 3 {
		t.Fatalf("tamanho no cabeçalho = %d", got)
	}
}
