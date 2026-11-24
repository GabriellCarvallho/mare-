package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSequencePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sequence.state")
	got, err := loadSequence(path)
	if err != nil || got != 0 {
		t.Fatalf("primeira leitura: sequência=%d erro=%v", got, err)
	}
	for _, sequence := range []uint32{1, 2, 3, 4, 5} {
		if err := saveSequence(path, sequence); err != nil {
			t.Fatalf("saveSequence(%d): %v", sequence, err)
		}
	}
	got, err = loadSequence(path)
	if err != nil || got != 5 {
		t.Fatalf("reabertura: sequência=%d erro=%v", got, err)
	}
}

func TestSequenceRecoversPreviousSlotWhenLatestIsCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sequence.state")
	if err := saveSequence(path, 0); err != nil {
		t.Fatal(err)
	}
	if err := saveSequence(path, 1); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("BAD!"), sequenceSlotSize); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := loadSequence(path)
	if err != nil || got != 0 {
		t.Fatalf("recuperação: sequência=%d erro=%v", got, err)
	}
}

func TestSequenceRejectsFileWithNoValidSlot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sequence.state")
	if err := os.WriteFile(path, make([]byte, sequenceSlotSize*2), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSequence(path); err == nil {
		t.Fatal("arquivo sem slot íntegro deveria ser rejeitado")
	}
}
