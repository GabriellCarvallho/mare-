// Package wal implementa o log persistente usado pelo broker.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrStorageFull = errors.New("limite de armazenamento atingido")
	ErrClosed      = errors.New("log fechado")
	ErrBroken      = errors.New("log inutilizado por erro de disco")

	errReplayRequired = errors.New("Replay deve ser chamado antes de Append ou Compact")
	errReplayDone     = errors.New("Replay já foi chamado")
)

// Record é o conteúdo opaco gravado pelo broker. Kind válido: 1 a 9.
type Record struct {
	Kind byte
	Data []byte
}

// Log serializa gravações e protege o arquivo para uso concorrente.
type Log interface {
	// Append bloqueia até sincronizar o registro no disco. A sequência cresce
	// sem reutilização; ErrStorageFull não grava nada. Após erro de disco,
	// esta e as próximas operações retornam ErrBroken.
	Append(r Record) (seq uint64, err error)

	// Replay deve ser chamado uma vez antes de Append. Entrega os registros
	// válidos em ordem. Um final inválido é truncado; erro de fn é repassado.
	Replay(fn func(seq uint64, r Record) error) error

	// Compact bloqueia Append, mantém os registros aceitos por keep e preserva
	// suas sequências. A substituição é feita por arquivo temporário + rename.
	Compact(keep func(seq uint64, r Record) bool) error

	// Close espera operações em andamento e fecha o arquivo.
	Close() error
}

const (
	// O maior Data do projeto tem cerca de 130 KiB. O limite de 1 MiB deixa
	// margem sem permitir alocações descontroladas durante a recuperação.
	maxRecordDataSize = 1 << 20
	headerSize        = 21
	kindCheckpoint    = 0 // registro interno; nunca é entregue ao broker
)

var fileMagic = [4]byte{'M', 'W', 'A', 'L'}
var crcTable = crc32.MakeTable(crc32.IEEE)

type fileLog struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	maxBytes int64
	size     int64
	nextSeq  uint64
	replayed bool
	closed   bool
	broken   bool
}

// Open abre ou cria o arquivo. maxBytes <= 0 significa sem limite.
func Open(path string, maxBytes int64) (Log, error) {
	if path == "" {
		return nil, errors.New("caminho do WAL vazio")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("criar diretório do WAL: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("abrir WAL: %w", err)
	}

	l := &fileLog{file: f, path: path, maxBytes: maxBytes, nextSeq: 1}
	high, end, err := l.scan(nil)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("recuperar WAL: %w", err)
	}
	l.size = end
	if high == ^uint64(0) {
		l.nextSeq = 0 // sinaliza que o espaço de sequências foi esgotado
	} else {
		l.nextSeq = high + 1
	}
	if maxBytes > 0 && end > maxBytes {
		_ = f.Close()
		return nil, ErrStorageFull
	}
	return l, nil
}

func (l *fileLog) Append(r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.stateError(); err != nil {
		return 0, err
	}
	if !l.replayed {
		return 0, errReplayRequired
	}
	if r.Kind == kindCheckpoint || r.Kind > 9 {
		return 0, fmt.Errorf("tipo de registro inválido: %d", r.Kind)
	}
	if len(r.Data) > maxRecordDataSize {
		return 0, fmt.Errorf("registro excede o limite de %d bytes", maxRecordDataSize)
	}
	if l.nextSeq == 0 {
		return 0, errors.New("sequência do WAL esgotada")
	}

	seq := l.nextSeq
	frame := encodeRecord(r.Kind, seq, r.Data)
	if l.maxBytes > 0 && l.size+int64(len(frame)) > l.maxBytes {
		return 0, ErrStorageFull
	}
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return 0, l.breakLog(err)
	}
	if err := writeFull(l.file, frame); err != nil {
		return 0, l.breakLog(err)
	}
	if err := l.file.Sync(); err != nil {
		return 0, l.breakLog(err)
	}

	l.size += int64(len(frame))
	if seq == ^uint64(0) {
		l.nextSeq = 0
	} else {
		l.nextSeq++
	}
	return seq, nil
}

func (l *fileLog) Replay(fn func(uint64, Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.stateError(); err != nil {
		return err
	}
	if l.replayed {
		return errReplayDone
	}
	if fn == nil {
		return errors.New("função Replay nil")
	}

	var callbackErr error
	high, end, err := l.scan(func(seq uint64, r Record) error {
		if callbackErr = fn(seq, r); callbackErr != nil {
			return callbackErr
		}
		return nil
	})
	if err != nil {
		if callbackErr != nil {
			return callbackErr
		}
		return l.breakLog(err)
	}
	l.size = end
	if high == ^uint64(0) {
		l.nextSeq = 0
	} else {
		l.nextSeq = high + 1
	}
	l.replayed = true
	return nil
}

func (l *fileLog) Compact(keep func(uint64, Record) bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.stateError(); err != nil {
		return err
	}
	if !l.replayed {
		return errReplayRequired
	}
	if keep == nil {
		return errors.New("função Compact nil")
	}

	dir := filepath.Dir(l.path)
	tmp, err := os.CreateTemp(dir, ".wal-compact-*")
	if err != nil {
		return l.breakLog(err)
	}
	tmpName := tmp.Name()
	removeTemp := true
	defer func() {
		_ = tmp.Close()
		if removeTemp {
			_ = os.Remove(tmpName)
		}
	}()

	var newSize int64
	var lastKept uint64
	var tempErr error
	high, end, err := l.scan(func(seq uint64, r Record) error {
		if !keep(seq, r) {
			return nil
		}
		frame := encodeRecord(r.Kind, seq, r.Data)
		if tempErr = writeFull(tmp, frame); tempErr != nil {
			return tempErr
		}
		newSize += int64(len(frame))
		lastKept = seq
		return nil
	})
	if err != nil {
		if tempErr != nil {
			return l.breakLog(tempErr)
		}
		return l.breakLog(err)
	}
	l.size = end // scan pode ter removido um trecho final inválido

	// Guarda a última sequência usada mesmo quando a compactação remove
	// justamente o registro mais recente. O marcador é interno ao WAL.
	if high > lastKept {
		frame := encodeRecord(kindCheckpoint, high, nil)
		if err := writeFull(tmp, frame); err != nil {
			return l.breakLog(err)
		}
		newSize += int64(len(frame))
	}
	if l.maxBytes > 0 && newSize > l.maxBytes {
		return ErrStorageFull
	}
	if err := tmp.Sync(); err != nil {
		return l.breakLog(err)
	}
	if err := tmp.Close(); err != nil {
		return l.breakLog(err)
	}

	// No Windows, o arquivo de destino precisa estar fechado antes do rename.
	if err := l.file.Close(); err != nil {
		l.file = nil
		return l.breakLog(err)
	}
	l.file = nil
	if err := os.Rename(tmpName, l.path); err != nil {
		// Tenta reabrir o arquivo antigo; o log fica bloqueado após a falha.
		l.file, _ = os.OpenFile(l.path, os.O_RDWR, 0o600)
		return l.breakLog(err)
	}
	removeTemp = false

	f, err := os.OpenFile(l.path, os.O_RDWR, 0o600)
	if err != nil {
		return l.breakLog(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return l.breakLog(err)
	}
	l.file = f
	l.size = newSize
	if high == ^uint64(0) {
		l.nextSeq = 0
	} else {
		l.nextSeq = high + 1
	}
	return nil
}

func (l *fileLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	if err != nil {
		l.broken = true
		return fmt.Errorf("%w: fechar arquivo: %v", ErrBroken, err)
	}
	return nil
}

func (l *fileLog) stateError() error {
	if l.closed {
		return ErrClosed
	}
	if l.broken {
		return ErrBroken
	}
	if l.file == nil {
		return ErrBroken
	}
	return nil
}

func (l *fileLog) breakLog(err error) error {
	l.broken = true
	return fmt.Errorf("%w: %v", ErrBroken, err)
}

// scan valida o arquivo inteiro, truncando no primeiro registro inválido.
// Marcadores internos Kind=0 atualizam a sequência máxima sem chegar ao broker.
func (l *fileLog) scan(fn func(uint64, Record) error) (high uint64, end int64, err error) {
	info, err := l.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()
	var offset int64

	for offset < size {
		if size-offset < headerSize {
			return high, offset, l.truncate(offset)
		}
		header := make([]byte, headerSize)
		if _, err := l.file.ReadAt(header, offset); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return high, offset, l.truncate(offset)
			}
			return high, offset, err
		}

		kind := header[4]
		seq := binary.BigEndian.Uint64(header[5:13])
		length := binary.BigEndian.Uint32(header[13:17])
		if string(header[:4]) != string(fileMagic[:]) || length > maxRecordDataSize || seq == 0 {
			return high, offset, l.truncate(offset)
		}
		if kind == kindCheckpoint {
			if length != 0 || seq < high {
				return high, offset, l.truncate(offset)
			}
		} else if kind > 9 || seq <= high {
			return high, offset, l.truncate(offset)
		}

		recordSize := int64(headerSize) + int64(length)
		if recordSize > size-offset {
			return high, offset, l.truncate(offset)
		}
		data := make([]byte, int(length))
		if length > 0 {
			if _, err := l.file.ReadAt(data, offset+headerSize); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return high, offset, l.truncate(offset)
				}
				return high, offset, err
			}
		}
		if binary.BigEndian.Uint32(header[17:21]) != checksum(header[:17], data) {
			return high, offset, l.truncate(offset)
		}

		high = seq
		if kind != kindCheckpoint && fn != nil {
			if err := fn(seq, Record{Kind: kind, Data: data}); err != nil {
				return high, offset, err
			}
		}
		offset += recordSize
	}
	return high, offset, nil
}

func (l *fileLog) truncate(offset int64) error {
	if err := l.file.Truncate(offset); err != nil {
		return err
	}
	return l.file.Sync()
}

func encodeRecord(kind byte, seq uint64, data []byte) []byte {
	b := make([]byte, headerSize+len(data))
	copy(b[:4], fileMagic[:])
	b[4] = kind
	binary.BigEndian.PutUint64(b[5:13], seq)
	binary.BigEndian.PutUint32(b[13:17], uint32(len(data)))
	copy(b[headerSize:], data)
	binary.BigEndian.PutUint32(b[17:21], checksum(b[:17], data))
	return b
}

func checksum(header, data []byte) uint32 {
	crc := crc32.Update(0, crcTable, header)
	return crc32.Update(crc, crcTable, data)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
