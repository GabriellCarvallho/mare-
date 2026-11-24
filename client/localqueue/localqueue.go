// Package localqueue mantém em disco as publicações que ainda não receberam ACK.
package localqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrQueueFull = errors.New("fila local cheia")
	ErrClosed    = errors.New("fila fechada")
	ErrCorrupt   = errors.New("arquivo da fila corrompido")
)

const (
	fileMagic        = "MPQ1"
	recordHeaderLen  = 13 // magic(4), tipo(1), tamanho(4), CRC32(4)
	recordAdd        = byte(1)
	recordRemove     = byte(2)
	recordCheckpoint = byte(3)
)

// Entry representa uma publicação pendente. ID é interno e não vai pela rede.
type Entry struct {
	ID      uint64
	Topic   string
	Flags   byte
	Payload []byte
}

// Queue é segura para uso simultâneo por várias goroutines.
type Queue interface {
	Append(topic string, flags byte, payload []byte) (uint64, error)
	Pending() ([]Entry, error)
	Remove(id uint64) error
	Close() error
}

type diskQueue struct {
	mu sync.Mutex

	file     *os.File
	path     string
	maxBytes int64

	pending     map[uint64]Entry
	pendingSize int64
	fileSize    int64
	lastID      uint64
	closed      bool
	broken      error
}

// Open abre ou cria a fila. maxBytes limita o tamanho das entradas pendentes.
func Open(path string, maxBytes int64) (Queue, error) {
	if path == "" || maxBytes <= 0 {
		return nil, errors.New("caminho inválido ou limite da fila não positivo")
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("abrir fila: %w", err)
	}

	q := &diskQueue{
		file: f, path: path, maxBytes: maxBytes,
		pending: make(map[uint64]Entry),
	}
	if err := q.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(q.fileSize, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("posicionar fila: %w", err)
	}
	return q, nil
}

func (q *diskQueue) Append(topic string, flags byte, payload []byte) (uint64, error) {
	if len(topic) > math.MaxUint16 {
		return 0, errors.New("nome do tópico excede 65535 bytes")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.check(); err != nil {
		return 0, err
	}
	if q.lastID == math.MaxUint64 {
		return 0, errors.New("IDs da fila esgotados")
	}

	size := int64(recordHeaderLen + 11 + len(topic) + len(payload))
	if size > q.maxBytes-q.pendingSize {
		return 0, ErrQueueFull
	}
	if uint64(11+len(topic))+uint64(len(payload)) > math.MaxUint32 {
		return 0, errors.New("entrada muito grande")
	}

	id := q.lastID + 1
	copyPayload := append([]byte(nil), payload...)
	record := encodeRecord(recordAdd, encodeAddBody(id, topic, flags, copyPayload))
	if err := q.writeRecord(record); err != nil {
		return 0, err
	}

	q.pending[id] = Entry{ID: id, Topic: topic, Flags: flags, Payload: copyPayload}
	q.pendingSize += size
	q.lastID = id
	return id, nil
}

func (q *diskQueue) Pending() ([]Entry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.check(); err != nil {
		return nil, err
	}

	ids := make([]uint64, 0, len(q.pending))
	for id := range q.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	entries := make([]Entry, 0, len(ids))
	for _, id := range ids {
		e := q.pending[id]
		e.Payload = append([]byte(nil), e.Payload...)
		entries = append(entries, e)
	}
	return entries, nil
}

func (q *diskQueue) Remove(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.check(); err != nil {
		return err
	}

	e, ok := q.pending[id]
	if !ok {
		return nil
	}
	record := encodeRecord(recordRemove, uint64Bytes(id))
	entrySize := entrySize(e)

	// Limita o crescimento do histórico de remoções.
	staleLimit := q.maxBytes/2 + 64*1024
	if q.fileSize+int64(len(record))-(q.pendingSize-entrySize) > staleLimit {
		if err := q.compactLocked(); err != nil {
			return fmt.Errorf("compactar fila: %w", err)
		}
	}
	if err := q.writeRecord(record); err != nil {
		return err
	}
	delete(q.pending, id)
	q.pendingSize -= entrySize
	return nil
}

func (q *diskQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.file.Close()
}

func (q *diskQueue) check() error {
	if q.closed {
		return ErrClosed
	}
	if q.broken != nil {
		return q.broken
	}
	return nil
}

// writeRecord só altera o estado em memória depois de gravar e sincronizar.
func (q *diskQueue) writeRecord(data []byte) error {
	start := q.fileSize
	if _, err := q.file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("posicionar log: %w", err)
	}
	if err := writeFull(q.file, data); err != nil {
		if undoErr := q.rollback(start); undoErr != nil {
			q.broken = fmt.Errorf("falha ao gravar (%v) e reverter (%w)", err, undoErr)
			return q.broken
		}
		return fmt.Errorf("gravar log: %w", err)
	}
	if err := q.file.Sync(); err != nil {
		q.broken = fmt.Errorf("sincronizar log: %w", err)
		return q.broken
	}
	q.fileSize += int64(len(data))
	return nil
}

func (q *diskQueue) rollback(size int64) error {
	if err := q.file.Truncate(size); err != nil {
		return err
	}
	if _, err := q.file.Seek(size, io.SeekStart); err != nil {
		return err
	}
	return q.file.Sync()
}

// replay recupera o estado e descarta apenas um registro final incompleto.
func (q *diskQueue) replay() error {
	info, err := q.file.Stat()
	if err != nil {
		return err
	}
	end := info.Size()

	for offset := int64(0); offset < end; {
		if end-offset < recordHeaderLen {
			return q.truncateTail(offset)
		}

		h := make([]byte, recordHeaderLen)
		if _, err := q.file.ReadAt(h, offset); err != nil {
			return err
		}
		if string(h[:4]) != fileMagic {
			return fmt.Errorf("%w: cabeçalho inválido no byte %d", ErrCorrupt, offset)
		}

		kind := h[4]
		bodySize := binary.BigEndian.Uint32(h[5:9])
		recordSize := int64(recordHeaderLen) + int64(bodySize)
		if end-offset < recordSize {
			return q.truncateTail(offset)
		}
		if int64(bodySize) > q.maxBytes {
			return fmt.Errorf("%w: registro excessivamente grande", ErrCorrupt)
		}

		body := make([]byte, int(bodySize))
		if _, err := q.file.ReadAt(body, offset+recordHeaderLen); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(h[9:13]) != checksum(kind, body) {
			return fmt.Errorf("%w: CRC inválido no byte %d", ErrCorrupt, offset)
		}
		if err := q.apply(kind, body, recordSize); err != nil {
			return fmt.Errorf("%w: registro no byte %d: %v", ErrCorrupt, offset, err)
		}
		offset += recordSize
	}

	q.fileSize = end
	return nil
}

func (q *diskQueue) truncateTail(offset int64) error {
	if err := q.file.Truncate(offset); err != nil {
		return err
	}
	if err := q.file.Sync(); err != nil {
		return err
	}
	q.fileSize = offset
	return nil
}

func (q *diskQueue) apply(kind byte, body []byte, size int64) error {
	switch kind {
	case recordAdd:
		if len(body) < 11 {
			return errors.New("ADD curto")
		}
		id := binary.BigEndian.Uint64(body[:8])
		topicSize := int(binary.BigEndian.Uint16(body[8:10]))
		if id == 0 || id <= q.lastID || len(body) < 11+topicSize {
			return errors.New("ADD inválido")
		}
		topicEnd := 10 + topicSize
		e := Entry{ID: id, Topic: string(body[10:topicEnd]), Flags: body[topicEnd], Payload: append([]byte(nil), body[topicEnd+1:]...)}
		if size > q.maxBytes-q.pendingSize {
			return errors.New("pendências excedem o limite")
		}
		q.pending[id] = e
		q.pendingSize += size
		q.lastID = id

	case recordRemove:
		if len(body) != 8 {
			return errors.New("REMOVE inválido")
		}
		id := binary.BigEndian.Uint64(body)
		e, ok := q.pending[id]
		if !ok {
			return errors.New("REMOVE sem ADD")
		}
		delete(q.pending, id)
		q.pendingSize -= entrySize(e)

	case recordCheckpoint:
		if len(body) != 8 {
			return errors.New("CHECKPOINT inválido")
		}
		id := binary.BigEndian.Uint64(body)
		if id < q.lastID {
			return errors.New("CHECKPOINT retrocede o ID")
		}
		q.lastID = id

	default:
		return fmt.Errorf("tipo desconhecido: %d", kind)
	}
	return nil
}

// compactLocked reescreve somente as pendências e preserva o último ID.
// O mutex da fila deve estar bloqueado durante a chamada.
func (q *diskQueue) compactLocked() error {
	tmp, err := os.CreateTemp(filepath.Dir(q.path), ".queue-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	ids := make([]uint64, 0, len(q.pending))
	for id := range q.pending {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var newSize int64
	for _, id := range ids {
		e := q.pending[id]
		r := encodeRecord(recordAdd, encodeAddBody(e.ID, e.Topic, e.Flags, e.Payload))
		if err := writeFull(tmp, r); err != nil {
			_ = tmp.Close()
			return err
		}
		newSize += int64(len(r))
	}
	if q.lastID > 0 {
		r := encodeRecord(recordCheckpoint, uint64Bytes(q.lastID))
		if err := writeFull(tmp, r); err != nil {
			_ = tmp.Close()
			return err
		}
		newSize += int64(len(r))
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	// No Windows, um arquivo aberto não pode ser renomeado. Fechamos o
	// temporário antes do rename e reabrimos o arquivo substituído depois.
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := q.file.Close(); err != nil {
		if reopenErr := q.reopenAfterCompact(); reopenErr != nil {
			q.broken = fmt.Errorf("fechar log falhou (%v) e reabrir falhou (%w)", err, reopenErr)
			return q.broken
		}
		return err
	}
	if err := os.Rename(tmpPath, q.path); err != nil {
		if reopenErr := q.reopenAfterCompact(); reopenErr != nil {
			q.broken = fmt.Errorf("rename falhou (%v) e reabrir log falhou (%w)", err, reopenErr)
			return q.broken
		}
		return err
	}

	f, err := os.OpenFile(q.path, os.O_RDWR, 0o600)
	if err != nil {
		q.broken = fmt.Errorf("reabrir log compactado: %w", err)
		return q.broken
	}
	if _, err := f.Seek(newSize, io.SeekStart); err != nil {
		_ = f.Close()
		q.broken = fmt.Errorf("posicionar log compactado: %w", err)
		return q.broken
	}
	q.file = f
	q.fileSize = newSize
	removeTmp = false // o rename já removeu o caminho temporário.
	return nil
}

func (q *diskQueue) reopenAfterCompact() error {
	f, err := os.OpenFile(q.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		_ = f.Close()
		return err
	}
	q.file = f
	q.fileSize = size
	return nil
}

func encodeAddBody(id uint64, topic string, flags byte, payload []byte) []byte {
	b := make([]byte, 11+len(topic)+len(payload))
	binary.BigEndian.PutUint64(b[:8], id)
	binary.BigEndian.PutUint16(b[8:10], uint16(len(topic)))
	copy(b[10:], topic)
	b[10+len(topic)] = flags
	copy(b[11+len(topic):], payload)
	return b
}

func encodeRecord(kind byte, body []byte) []byte {
	b := make([]byte, recordHeaderLen+len(body))
	copy(b[:4], fileMagic)
	b[4] = kind
	binary.BigEndian.PutUint32(b[5:9], uint32(len(body)))
	binary.BigEndian.PutUint32(b[9:13], checksum(kind, body))
	copy(b[recordHeaderLen:], body)
	return b
}

func checksum(kind byte, body []byte) uint32 {
	crc := crc32.Update(0, crc32.IEEETable, []byte{kind})
	return crc32.Update(crc, crc32.IEEETable, body)
}

func entrySize(e Entry) int64 {
	return int64(recordHeaderLen + 11 + len(e.Topic) + len(e.Payload))
}

func uint64Bytes(value uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	return b
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
