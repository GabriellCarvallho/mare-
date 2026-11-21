// maredump converte quadros do Maré Pub/Sub de hexadecimal para texto
// (ferramenta de diagnóstico da Seção 3.3).
//
// Uso:
//
//	maredump 000F01000100070000019C6AE1F1D00A96
//	maredump -payload temperatura "000F010001 00070000019C6AE1F1D00A96"
//	echo "000103 0005040000002A" | maredump
//
// Aceita um ou vários quadros concatenados; espaços e ":" são ignorados.
// Sem argumentos, lê o hexadecimal da entrada padrão.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

func main() {
	payloadFmt := flag.String("payload", "", `interpreta o payload: "temperatura" (12 bytes)`)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "uso: maredump [-payload temperatura] [HEX ...]")
		flag.PrintDefaults()
	}
	flag.Parse()

	var text string
	if flag.NArg() > 0 {
		text = strings.Join(flag.Args(), "")
	} else {
		in, err := io.ReadAll(os.Stdin)
		if err != nil {
			fail(err)
		}
		text = string(in)
	}
	raw, err := parseHex(text)
	if err != nil {
		fail(err)
	}
	if err := dump(os.Stdout, raw, *payloadFmt); err != nil {
		fail(err)
	}
}

func parseHex(s string) ([]byte, error) {
	clean := strings.NewReplacer(" ", "", "\n", "", "\r", "", "\t", "", ":", "").Replace(s)
	clean = strings.TrimPrefix(strings.TrimPrefix(clean, "0x"), "0X")
	b, err := hex.DecodeString(clean)
	if err != nil {
		return nil, fmt.Errorf("hexadecimal inválido: %w", err)
	}
	if len(b) == 0 {
		return nil, errors.New("nenhum byte informado")
	}
	return b, nil
}

func dump(w io.Writer, raw []byte, payloadFmt string) error {
	r := bytes.NewReader(raw)
	for i := 1; ; i++ {
		offset := len(raw) - r.Len()
		f, err := protocol.ReadFrame(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("quadro #%d (byte %d): %w", i, offset, err)
		}
		size := len(raw) - r.Len() - offset
		fmt.Fprintf(w, "#%d  %d bytes  % X\n    %s\n", i, size, raw[offset:offset+size], f)

		if payloadFmt != "" && (f.Type == protocol.TypePublish || f.Type == protocol.TypeMessage) {
			desc, err := describePayload(payloadFmt, f.Payload)
			if err != nil {
				fmt.Fprintf(w, "    payload: %v\n", err)
			} else {
				fmt.Fprintf(w, "    payload: %s\n", desc)
			}
		}
	}
}

// describePayload interpreta o payload da aplicação. O broker não faz isso:
// é só uma conveniência de diagnóstico.
func describePayload(format string, p []byte) (string, error) {
	switch format {
	case "temperatura":
		// sensor_id uint16 | sequence uint32 | timestamp uint32 | temp int16
		if len(p) != 12 {
			return "", fmt.Errorf("payload de temperatura deve ter 12 bytes, tem %d", len(p))
		}
		sensor := binary.BigEndian.Uint16(p[0:])
		seq := binary.BigEndian.Uint32(p[2:])
		ts := time.Unix(int64(binary.BigEndian.Uint32(p[6:])), 0).UTC()
		temp := float64(int16(binary.BigEndian.Uint16(p[10:]))) / 100
		return fmt.Sprintf("sensor_id=%d sequence=%d timestamp=%s temperatura=%.2f °C",
			sensor, seq, ts.Format(time.RFC3339), temp), nil
	}
	return "", fmt.Errorf("formato de payload desconhecido %q", format)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "maredump:", err)
	os.Exit(1)
}
