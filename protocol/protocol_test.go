package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"testing/iotest"
)

// ---------------------------------------------------------------------------
// Vetores compartilhados (testdata/vectors.json)

type vectorFrame struct {
	Type       byte   `json:"type"`
	Flags      byte   `json:"flags"`
	TopicID    uint8  `json:"topic_id"`
	DeliveryID uint32 `json:"delivery_id"`
	ClientID   uint16 `json:"client_id"`
	Version    byte   `json:"version"`
	Role       byte   `json:"role"`
	Epoch      uint32 `json:"epoch"`
	TopicName  string `json:"topic_name"`
	ErrCode    byte   `json:"err_code"`
	Payload    string `json:"payload"`
}

type vector struct {
	Name  string      `json:"name"`
	Hex   string      `json:"hex"`
	Frame vectorFrame `json:"frame"`
}

type vectorFile struct {
	Valid     []vector `json:"valid"`
	Invalid   []vector `json:"invalid"`
	Truncated []vector `json:"truncated"`
}

func loadVectors(t testing.TB) vectorFile {
	t.Helper()
	data, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vf vectorFile
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatal(err)
	}
	return vf
}

func mustHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatalf("hex inválido %q: %v", s, err)
	}
	return b
}

func (v vectorFrame) toFrame(t testing.TB) Frame {
	f := Frame{
		Type: v.Type, Flags: v.Flags, TopicID: v.TopicID, DeliveryID: v.DeliveryID,
		ClientID: v.ClientID, Version: v.Version, Role: v.Role, Epoch: v.Epoch,
		TopicName: v.TopicName, ErrCode: v.ErrCode,
	}
	if v.Payload != "" {
		f.Payload = mustHex(t, v.Payload)
	}
	return f
}

func equalFrames(a, b Frame) bool {
	return a.Type == b.Type && a.Flags == b.Flags && a.TopicID == b.TopicID &&
		a.DeliveryID == b.DeliveryID && a.ClientID == b.ClientID &&
		a.Version == b.Version && a.Role == b.Role && a.Epoch == b.Epoch &&
		a.TopicName == b.TopicName && a.ErrCode == b.ErrCode &&
		bytes.Equal(a.Payload, b.Payload)
}

func TestVectorsValid(t *testing.T) {
	for _, v := range loadVectors(t).Valid {
		t.Run(v.Name, func(t *testing.T) {
			raw := mustHex(t, v.Hex)
			want := v.Frame.toFrame(t)

			// bytes -> Frame
			got, err := ReadFrame(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if !equalFrames(got, want) {
				t.Fatalf("decodificado\n got %+v\nwant %+v", got, want)
			}
			// Frame -> bytes
			enc, err := Encode(want)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !bytes.Equal(enc, raw) {
				t.Fatalf("codificado\n got %X\nwant %X", enc, raw)
			}
		})
	}
}

func TestVectorsInvalid(t *testing.T) {
	for _, v := range loadVectors(t).Invalid {
		t.Run(v.Name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(mustHex(t, v.Hex)))
			if !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("esperado ErrInvalidFrame, obtido %v", err)
			}
		})
	}
}

func TestVectorsTruncated(t *testing.T) {
	for _, v := range loadVectors(t).Truncated {
		t.Run(v.Name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(mustHex(t, v.Hex)))
			if err != io.ErrUnexpectedEOF {
				t.Fatalf("esperado io.ErrUnexpectedEOF, obtido %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tamanhos exigidos pela divisão de tarefas (Parte 1)

func TestFrameSizes(t *testing.T) {
	pl := make([]byte, 12) // payload de temperatura
	cases := []struct {
		f    Frame
		want int
	}{
		{Frame{Type: TypePublish, TopicID: 1, Payload: pl}, 17},
		{Frame{Type: TypeMessage, TopicID: 1, DeliveryID: 1, Payload: pl}, 21},
		{Frame{Type: TypePublishAck}, 3},
		{Frame{Type: TypeDeliveryAck, DeliveryID: 1}, 7},
	}
	for _, c := range cases {
		b, err := Encode(c.f)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) != c.want {
			t.Errorf("%s: %d bytes, esperado %d", TypeName(c.f.Type), len(b), c.want)
		}
	}
}

// Campos que o tipo não usa são ignorados na codificação.
func TestEncodeIgnoresUnusedFields(t *testing.T) {
	b, err := Encode(Frame{Type: TypePublishAck, TopicID: 9, Payload: []byte{1}, ClientID: 3})
	if err != nil || !bytes.Equal(b, []byte{0, 1, TypePublishAck}) {
		t.Fatalf("%X %v", b, err)
	}
}

func TestEncodeErrors(t *testing.T) {
	rangeCases := map[string]Frame{
		"nome com 64 bytes":    {Type: TypeCreateTopic, TopicName: strings.Repeat("a", 64)},
		"nome vazio":           {Type: TypeResolveTopic},
		"nome UTF-8 inválido":  {Type: TypeCreateTopic, TopicName: "\xC3\x28"},
		"topic_id 0":           {Type: TypeSubscribe},
		"flag reservada":       {Type: TypePublish, Flags: 0x80, TopicID: 1, Payload: []byte{1}},
		"payload vazio":        {Type: TypePublish, TopicID: 1},
		"payload de 250 bytes": {Type: TypePublish, TopicID: 1, Payload: make([]byte, 250)},
		"papel desconhecido":   {Type: TypeStatus, Role: 9},
		"código desconhecido":  {Type: TypeError, ErrCode: 0xEE},
	}
	for name, f := range rangeCases {
		if _, err := Encode(f); !errors.Is(err, ErrFieldRange) {
			t.Errorf("%s: esperado ErrFieldRange, obtido %v", name, err)
		}
	}
	if _, err := Encode(Frame{Type: 0x7F}); !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("tipo desconhecido: esperado ErrInvalidFrame, obtido %v", err)
	}
}

// 249 bytes de payload é o máximo: o MESSAGE correspondente fica com 256.
func TestMaxPayload(t *testing.T) {
	pl := make([]byte, MaxPayloadSize)
	if _, err := Encode(Frame{Type: TypePublish, TopicID: 1, Payload: pl}); err != nil {
		t.Fatal(err)
	}
	b, err := Encode(Frame{Type: TypeMessage, TopicID: 1, Payload: pl})
	if err != nil || len(b)-LengthPrefixSize != MaxBodySize {
		t.Fatalf("MESSAGE máximo: %d bytes, %v", len(b), err)
	}
}

// Dois quadros entregues 1 byte por vez: uma leitura do socket não
// contém necessariamente um quadro inteiro.
func TestReadFrameOneByteAtATime(t *testing.T) {
	f1 := Frame{Type: TypePublish, TopicID: 1, Payload: []byte{1, 2, 3}}
	f2 := Frame{Type: TypePublishAck}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, f1); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, f2); err != nil {
		t.Fatal(err)
	}
	r := iotest.OneByteReader(&buf)
	for i, want := range []Frame{f1, f2} {
		got, err := ReadFrame(r)
		if err != nil || !equalFrames(got, want) {
			t.Fatalf("quadro %d: %+v, %v", i, got, err)
		}
	}
	if _, err := ReadFrame(r); err != io.EOF {
		t.Fatalf("esperado io.EOF entre quadros, obtido %v", err)
	}
}

func TestBodyOver256Rejected(t *testing.T) {
	raw := append([]byte{0x01, 0x01}, make([]byte, 257)...)
	if _, err := ReadFrame(bytes.NewReader(raw)); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("esperado ErrInvalidFrame, obtido %v", err)
	}
}

func TestAppendEncodeNoAlloc(t *testing.T) {
	f := Frame{Type: TypePublish, TopicID: 1, Payload: make([]byte, 12)}
	buf := make([]byte, 0, 64)
	allocs := testing.AllocsPerRun(1000, func() { buf, _ = AppendEncode(buf[:0], f) })
	if allocs != 0 {
		t.Fatalf("AppendEncode alocou %.0f vezes", allocs)
	}
}

// Seção 3.3: mede o tamanho do JSON equivalente.
func TestCompareWithJSON(t *testing.T) {
	j, _ := json.Marshal(map[string]any{
		"type": "PUBLISH", "topic": "mar/temperatura/cabedelo",
		"sensor_id": 7, "sequence": 412, "timestamp": "2026-10-28T13:00:00Z",
		"priority": "NORMAL", "temperature_c": 27.10,
	})
	bin, _ := Encode(Frame{Type: TypePublish, TopicID: 1, Payload: make([]byte, 12)})
	total := len(j) + LengthPrefixSize
	t.Logf("JSON %d bytes no TCP | binário %d | redução %.1f%%",
		total, len(bin), 100*(1-float64(len(bin))/float64(total)))
}

// Nenhuma entrada pode causar pânico, e tudo que decodifica deve
// recodificar para os mesmos bytes.
func FuzzDecode(f *testing.F) {
	for _, v := range loadVectors(f).Valid {
		f.Add(mustHex(f, v.Hex)[LengthPrefixSize:])
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		fr, err := Decode(body)
		if err != nil {
			if !errors.Is(err, ErrInvalidFrame) {
				t.Fatalf("erro fora do contrato: %v", err)
			}
			return
		}
		enc, err := Encode(fr)
		if err != nil || !bytes.Equal(enc[LengthPrefixSize:], body) {
			t.Fatalf("%X decodifica mas recodifica como %X (%v)", body, enc, err)
		}
	})
}

func BenchmarkAppendEncode(b *testing.B) {
	f := Frame{Type: TypePublish, TopicID: 1, Payload: make([]byte, 12)}
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, _ = AppendEncode(buf[:0], f)
	}
}

func BenchmarkDecode(b *testing.B) {
	body := mustHex(b, "010001 00070000019C6AE1F1D00A96")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Decode(body)
	}
}
