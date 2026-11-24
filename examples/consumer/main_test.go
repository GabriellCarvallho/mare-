package main

import (
	"encoding/hex"
	"testing"
)

func TestDecodeReadingMatchesProjectVector(t *testing.T) {
	payload, err := hex.DecodeString("00070000019c6ae1f1d00a96")
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeReading(payload)
	if err != nil {
		t.Fatalf("decodeReading(): %v", err)
	}
	if got.SensorID != 7 || got.Sequence != 412 || got.Timestamp != 0x6AE1F1D0 || got.TemperatureC != 27.10 {
		t.Fatalf("leitura decodificada incorretamente: %+v", got)
	}
}

func TestDecodeReadingRejectsWrongLength(t *testing.T) {
	if _, err := decodeReading([]byte{1, 2, 3}); err == nil {
		t.Fatal("payload com tamanho inválido deveria ser rejeitado")
	}
}
