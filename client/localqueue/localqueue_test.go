package localqueue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func openTestQueue(t *testing.T, maxBytes int64) (Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sensor.queue")
	q, err := Open(path, maxBytes)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return q, path
}

func TestAppendPendingAndReopen(t *testing.T) {
	q, path := openTestQueue(t, 1<<20)
	payload := []byte{0, 7, 0, 0, 1, 2}
	id, err := q.Append("temperatura", 1, payload)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("Append() ID = %d, want 1", id)
	}
	payload[0] = 99 // A fila deve ter guardado uma cópia.

	pending, err := q.Pending()
	if err != nil {
		t.Fatalf("Pending() error = %v", err)
	}
	if len(pending) != 1 || pending[0].Payload[0] != 0 {
		t.Fatalf("Pending() = %#v; a cópia do payload não foi preservada", pending)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	q2, err := Open(path, 1<<20)
	if err != nil {
		t.Fatalf("reabrir fila: %v", err)
	}
	defer q2.Close()
	pending, err = q2.Pending()
	if err != nil {
		t.Fatalf("Pending() após reabrir: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != 1 || pending[0].Topic != "temperatura" || pending[0].Flags != 1 || !bytes.Equal(pending[0].Payload, []byte{0, 7, 0, 0, 1, 2}) {
		t.Fatalf("entrada recuperada incorretamente: %#v", pending)
	}
}

func TestQueueFullAndRemoveFreesCapacity(t *testing.T) {
	// Um ADD com tópico "t" e payload de 25 bytes ocupa exatamente 50 bytes.
	q, _ := openTestQueue(t, 50)
	id, err := q.Append("t", 0, bytes.Repeat([]byte{1}, 25))
	if err != nil {
		t.Fatalf("Append() na capacidade exata: %v", err)
	}
	if _, err := q.Append("t", 0, nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Append() acima do limite = %v, want ErrQueueFull", err)
	}
	if err := q.Remove(id); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := q.Append("t", 0, nil); err != nil {
		t.Fatalf("Append() depois de liberar capacidade: %v", err)
	}
	_ = q.Close()
}

func TestRemoveOutOfOrderAndIDsRemainIncreasing(t *testing.T) {
	q, path := openTestQueue(t, 1<<20)
	for i := 0; i < 4; i++ {
		if _, err := q.Append("temperatura", 0, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Remove(3); err != nil {
		t.Fatal(err)
	}
	if err := q.Remove(1); err != nil {
		t.Fatal(err)
	}
	if err := q.Remove(99); err != nil {
		t.Fatalf("Remove() de ID inexistente deve ser nil: %v", err)
	}
	pending, err := q.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].ID != 2 || pending[1].ID != 4 {
		t.Fatalf("pendentes = %#v, IDs esperados [2, 4]", pending)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()
	id, err := q2.Append("temperatura", 0, []byte("nova"))
	if err != nil {
		t.Fatal(err)
	}
	if id != 5 {
		t.Fatalf("novo ID = %d, want 5", id)
	}
}

func TestTruncatedTailIsDiscarded(t *testing.T) {
	q, path := openTestQueue(t, 1<<20)
	if _, err := q.Append("t", 0, []byte("primeira")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Append("t", 0, []byte("segunda")); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(path, 1<<20)
	if err != nil {
		t.Fatalf("Open() deveria descartar o final incompleto: %v", err)
	}
	defer q2.Close()
	pending, err := q2.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || string(pending[0].Payload) != "primeira" {
		t.Fatalf("pendentes após recuperar final cortado = %#v", pending)
	}
}

func TestCorruptRecordInMiddleReturnsErrCorrupt(t *testing.T) {
	q, path := openTestQueue(t, 1<<20)
	for _, value := range []string{"primeira", "segunda", "terceira"} {
		if _, err := q.Append("t", 0, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Corrompe o CRC do primeiro registro, que não é o final do arquivo.
	if _, err := f.WriteAt([]byte{0xFF}, 9); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, 1<<20); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open() com registro corrompido = %v, want ErrCorrupt", err)
	}
}

func TestCompactPreservesPendingEntriesAndLastID(t *testing.T) {
	q, path := openTestQueue(t, 1<<20)
	for i := 0; i < 4; i++ {
		if _, err := q.Append("t", 0, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Remove(1); err != nil {
		t.Fatal(err)
	}
	if err := q.Remove(3); err != nil {
		t.Fatal(err)
	}

	dq := q.(*diskQueue)
	dq.mu.Lock()
	err := dq.compactLocked()
	dq.mu.Unlock()
	if err != nil {
		t.Fatalf("compactLocked() error = %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(path, 1<<20)
	if err != nil {
		t.Fatalf("Open() depois da compactação: %v", err)
	}
	defer q2.Close()
	pending, err := q2.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].ID != 2 || pending[1].ID != 4 {
		t.Fatalf("pendentes após compactação = %#v", pending)
	}
	id, err := q2.Append("t", 0, []byte("quinta"))
	if err != nil {
		t.Fatal(err)
	}
	if id != 5 {
		t.Fatalf("ID depois da compactação = %d, want 5", id)
	}
}

func TestRemoveAutomaticallyCompactsHistory(t *testing.T) {
	q, path := openTestQueue(t, 128)
	for i := 0; i < 1500; i++ {
		id, err := q.Append("t", 0, []byte{1})
		if err != nil {
			t.Fatalf("Append() na iteração %d: %v", i, err)
		}
		if err := q.Remove(id); err != nil {
			t.Fatalf("Remove() na iteração %d: %v", i, err)
		}
	}

	dq := q.(*diskQueue)
	dq.mu.Lock()
	physicalSize := dq.fileSize
	dq.mu.Unlock()
	if physicalSize >= 70*1024 {
		t.Fatalf("histórico não foi compactado: arquivo com %d bytes", physicalSize)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q2, err := Open(path, 128)
	if err != nil {
		t.Fatalf("reabrir após compactação automática: %v", err)
	}
	defer q2.Close()
	id, err := q2.Append("t", 0, []byte{2})
	if err != nil {
		t.Fatal(err)
	}
	if id != 1501 {
		t.Fatalf("ID após compactação automática = %d, want 1501", id)
	}
}

func TestConcurrentAppend(t *testing.T) {
	q, _ := openTestQueue(t, 10<<20)
	const workers = 12
	const each = 40

	var wg sync.WaitGroup
	ids := make(chan uint64, workers*each)
	errs := make(chan error, workers*each)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				id, err := q.Append("temperatura", 0, []byte{byte(worker), byte(i)})
				if err != nil {
					errs <- err
					return
				}
				ids <- id
			}
		}(worker)
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("Append() concorrente: %v", err)
	}

	allIDs := make([]uint64, 0, workers*each)
	for id := range ids {
		allIDs = append(allIDs, id)
	}
	sort.Slice(allIDs, func(i, j int) bool { return allIDs[i] < allIDs[j] })
	if len(allIDs) != workers*each {
		t.Fatalf("IDs recebidos = %d, want %d", len(allIDs), workers*each)
	}
	for i, id := range allIDs {
		if id != uint64(i+1) {
			t.Fatalf("IDs não são sequenciais na posição %d: %d", i, id)
		}
	}
	pending, err := q.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != workers*each {
		t.Fatalf("Pending() retornou %d entradas, want %d", len(pending), workers*each)
	}
	_ = q.Close()
}

func TestMethodsAfterCloseReturnErrClosed(t *testing.T) {
	q, _ := openTestQueue(t, 1<<20)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Append("t", 0, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append() após Close = %v, want ErrClosed", err)
	}
	if _, err := q.Pending(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pending() após Close = %v, want ErrClosed", err)
	}
	if err := q.Remove(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remove() após Close = %v, want ErrClosed", err)
	}
}

func TestCheckpointRecordUsesBigEndianLastID(t *testing.T) {
	body := make([]byte, 8)
	binary.BigEndian.PutUint64(body, 0x0102030405060708)
	record := encodeRecord(recordCheckpoint, body)
	if got := binary.BigEndian.Uint64(record[recordHeaderLen:]); got != 0x0102030405060708 {
		t.Fatalf("checkpoint = %#x", got)
	}
}
