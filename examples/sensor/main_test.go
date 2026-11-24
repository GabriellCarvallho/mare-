package main

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestEncodeReadingMatchesProjectVector(t *testing.T) {
	want, err := hex.DecodeString("00070000019c6ae1f1d00a96")
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeReading(7, 412, 0x6AE1F1D0, 27.10)
	if err != nil {
		t.Fatalf("encodeReading(): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload = % X, esperado % X", got, want)
	}
}

func TestEncodeReadingRejectsTemperatureOutOfRange(t *testing.T) {
	if _, err := encodeReading(7, 1, 1, 400); err == nil {
		t.Fatal("temperatura fora do intervalo deveria ser rejeitada")
	}
}
