package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

func TestDumpDocExample(t *testing.T) {
	raw, err := parseHex("000F010001 00070000019C6AE1F1D00A96")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := dump(&out, raw, "temperatura"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"17 bytes", "PUBLISH prioridade=NORMAL topic_id=1",
		"sensor_id=7 sequence=412 timestamp=2026-10-28T13:00:00Z temperatura=27.10 °C",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("saída sem %q:\n%s", want, out.String())
		}
	}
}

func TestDumpSeveralFrames(t *testing.T) {
	raw, _ := parseHex("00:01:03 0005040000002A 00023010")
	var out bytes.Buffer
	if err := dump(&out, raw, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"#1", "PUBLISH_ACK", "#2", "DELIVERY_ACK delivery_id=42", "#3", "NO_SUBSCRIBERS"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("saída sem %q:\n%s", want, out.String())
		}
	}
}

func TestDumpInvalid(t *testing.T) {
	raw, _ := parseHex("000103 00017F")
	var out bytes.Buffer
	err := dump(&out, raw, "")
	if !errors.Is(err, protocol.ErrInvalidFrame) || !strings.Contains(err.Error(), "quadro #2") {
		t.Fatalf("esperado erro no quadro #2, obtido %v", err)
	}
	if _, err := parseHex("zz"); err == nil {
		t.Fatal("hex inválido deveria falhar")
	}
}
