package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const sequenceSlotSize = 12

var sequenceMagic = [4]byte{'M', 'S', 'E', 'Q'}

// loadSequence lê os dois slots alternados e escolhe o maior contador íntegro.
// Um slot válido sobrevive se a gravação do slot seguinte for interrompida.
func loadSequence(path string) (uint32, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := saveSequence(path, 0); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() == 0 {
		if err := saveSequence(path, 0); err != nil {
			return 0, err
		}
		return 0, nil
	}

	var highest uint32
	valid := false
	for slot := int64(0); slot < 2; slot++ {
		var record [sequenceSlotSize]byte
		n, readErr := file.ReadAt(record[:], slot*sequenceSlotSize)
		if readErr != nil && readErr != io.EOF {
			return 0, readErr
		}
		if n != sequenceSlotSize || !validSequenceRecord(record[:]) {
			continue
		}
		value := binary.BigEndian.Uint32(record[4:8])
		if !valid || value > highest {
			highest = value
			valid = true
		}
	}
	if !valid {
		return 0, errors.New("arquivo de sequência corrompido: nenhum slot íntegro")
	}
	return highest, nil
}

// saveSequence alterna entre dois slots. O slot anterior permanece válido
// durante a gravação e o Sync do novo contador.
func saveSequence(path string, sequence uint32) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()

	var record [sequenceSlotSize]byte
	copy(record[:4], sequenceMagic[:])
	binary.BigEndian.PutUint32(record[4:8], sequence)
	binary.BigEndian.PutUint32(record[8:12], crc32.ChecksumIEEE(record[:8]))

	slot := int64(sequence%2) * sequenceSlotSize
	n, err := file.WriteAt(record[:], slot)
	if err != nil {
		return err
	}
	if n != len(record) {
		return fmt.Errorf("gravação incompleta do contador: %d de %d bytes", n, len(record))
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

func validSequenceRecord(record []byte) bool {
	if len(record) != sequenceSlotSize || string(record[:4]) != string(sequenceMagic[:]) {
		return false
	}
	want := binary.BigEndian.Uint32(record[8:12])
	return crc32.ChecksumIEEE(record[:8]) == want
}
