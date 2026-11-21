package logrec

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	tests := []Record{
		{
			Kind:      KindTopicCreated,
			TopicID:   1,
			TopicName: "temperatura",
		},
		{
			Kind:    KindTopicRemoving,
			TopicID: 2,
		},
		{
			Kind:    KindTopicPurged,
			TopicID: 3,
		},
		{
			Kind:    KindSubscribed,
			TopicID: 4,
			Client:  1000,
		},
		{
			Kind:    KindUnsubscribed,
			TopicID: 5,
			Client:  2000,
		},
		{
			Kind:       KindPublished,
			Ref:        123456789,
			TopicID:    6,
			Flags:      1,
			Publisher:  100,
			Recipients: []uint16{10, 20, 30},
			Payload:    []byte{0x01, 0x02, 0x03, 0x04},
		},
		{
			Kind:   KindDelivered,
			Ref:    987654321,
			Client: 100,
		},
		{
			Kind:   KindQuarantined,
			Ref:    987654322,
			Client: 200,
		},
		{
			Kind:  KindEpoch,
			Epoch: 123456,
		},
	}

	for _, original := range tests {
		t.Run("Kind_"+string(rune('0'+original.Kind)), func(t *testing.T) {
			encoded, err := Encode(original)
			if err != nil {
				t.Fatalf("Encode falhou: %v", err)
			}

			decoded, err := Decode(original.Kind, encoded)
			if err != nil {
				t.Fatalf("Decode falhou: %v", err)
			}

			if !reflect.DeepEqual(original, decoded) {
				t.Errorf(
					"registros diferentes:\nesperado: %+v\nrecebido: %+v",
					original,
					decoded,
				)
			}
		})
	}
}

func TestDecodeCorrupt(t *testing.T) {
	tests := []struct {
		name string
		kind byte
		data []byte
	}{
		{"TopicCreated incompleto", KindTopicCreated, []byte{1}},
		{"TopicCreated nome incompleto", KindTopicCreated, []byte{1, 5, 'a'}},
		{"TopicCreated nome vazio", KindTopicCreated, []byte{1, 0}},
		{"TopicCreated topic_id zero", KindTopicCreated, []byte{0, 1, 'a'}},
		{"TopicRemoving vazio", KindTopicRemoving, []byte{}},
		{"TopicRemoving topic_id zero", KindTopicRemoving, []byte{0}},
		{"Subscribed incompleto", KindSubscribed, []byte{1, 2}},
		{"Subscribed com bytes extras", KindSubscribed, []byte{1, 2, 3, 4}},
		{"Subscribed topic_id zero", KindSubscribed, []byte{0, 0, 1}},
		{"TopicPurged incompleto", KindTopicPurged, []byte{}},
		{"Unsubscribed incompleto", KindUnsubscribed, []byte{1, 2}},
		{"Epoch incompleto", KindEpoch, []byte{1, 2, 3}},
		{"Delivered incompleto", KindDelivered, []byte{1, 2, 3}},
		{"Quarantined incompleto", KindQuarantined, []byte{1, 2, 3}},
		{"Tipo desconhecido", 99, []byte{1}},

		// PUBLISHED
		{"Published cabeçalho incompleto", KindPublished, []byte{1, 2, 3}},
		{"Published destinatário incompleto", KindPublished,
			[]byte{
				0, 0, 0, 0, 0, 0, 0, 1,
				1, 0,
				0, 1,
				0, 1,
				0,
			}},
		{"Published payload incompleto", KindPublished,
			[]byte{
				0, 0, 0, 0, 0, 0, 0, 1,
				1, 0,
				0, 1,
				0, 0,
				3, 1, 2,
			}},
		{"Published bytes extras", KindPublished,
			[]byte{
				0, 0, 0, 0, 0, 0, 0, 1,
				1, 0,
				0, 1,
				0, 0,
				1, 1, 2,
			}},
		{"Published topic_id zero", KindPublished,
			[]byte{
				0, 0, 0, 0, 0, 0, 0, 1,
				0, 0,
				0, 1,
				0, 0,
				0,
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(tt.kind, tt.data)

			if !errors.Is(err, ErrCorrupt) {
				t.Errorf("esperado ErrCorrupt, recebido: %v", err)
			}
		})
	}
}

func TestEncodeInvalid(t *testing.T) {
	tests := []struct {
		name   string
		record Record
	}{
		{"payload acima do limite", Record{Kind: KindPublished, TopicID: 1, Payload: make([]byte, MaxPayload+1)}},
		{"nome acima do limite", Record{Kind: KindTopicCreated, TopicID: 1, TopicName: strings.Repeat("a", MaxTopicName+1)}},
		{"nome vazio", Record{Kind: KindTopicCreated, TopicID: 1}},
		{"nome UTF-8 inválido", Record{Kind: KindTopicCreated, TopicID: 1, TopicName: string([]byte{0xff, 0xfe})}},
		{"topic_id zero em TOPIC_CREATED", Record{Kind: KindTopicCreated, TopicName: "a"}},
		{"topic_id zero em SUBSCRIBED", Record{Kind: KindSubscribed, Client: 1}},
		{"topic_id zero em PUBLISHED", Record{Kind: KindPublished, Payload: []byte{1}}},
		{"tipo desconhecido", Record{Kind: 99}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Encode(tt.record); !errors.Is(err, ErrCorrupt) {
				t.Errorf("esperado ErrCorrupt, recebido: %v", err)
			}
		})
	}
}

func TestEncodeBytesExatos(t *testing.T) {
	record := Record{
		Kind:    KindSubscribed,
		TopicID: 4,
		Client:  1000,
	}

	expected := []byte{
		4,    // TopicID
		0x03, // Primeiro byte do Client
		0xE8, // Segundo byte do Client
	}

	encoded, err := Encode(record)
	if err != nil {
		t.Fatalf("Encode falhou: %v", err)
	}

	if !bytes.Equal(encoded, expected) {
		t.Errorf("bytes incorretos: esperado %v, recebido %v",
			expected, encoded)
	}
}

// Trava o formato do PUBLISHED, que é usado no teste de integração com o wal.
func TestPublishedBytesExatos(t *testing.T) {
	record := Record{
		Kind:       KindPublished,
		Ref:        1,
		TopicID:    2,
		Flags:      1,
		Publisher:  7,
		Recipients: []uint16{10, 20},
		Payload:    []byte{0xAA, 0xBB},
	}

	expected := []byte{
		0, 0, 0, 0, 0, 0, 0, 1, // ref
		2, 1, // topic_id, flags
		0, 7, // publicador
		0, 2, 0, 10, 0, 20, // 2 destinatários: 10 e 20
		2, 0xAA, 0xBB, // payload com 2 bytes
	}

	encoded, err := Encode(record)
	if err != nil {
		t.Fatalf("Encode falhou: %v", err)
	}

	if !bytes.Equal(encoded, expected) {
		t.Errorf("bytes incorretos:\nesperado % X\nrecebido % X", expected, encoded)
	}
}

func TestPublishedVazio(t *testing.T) {
	tests := []Record{
		{
			Kind:       KindPublished,
			Ref:        1,
			TopicID:    1,
			Publisher:  10,
			Recipients: []uint16{},
			Payload:    []byte{1, 2, 3},
		},
		{
			Kind:       KindPublished,
			Ref:        2,
			TopicID:    1,
			Publisher:  10,
			Recipients: []uint16{20},
			Payload:    []byte{},
		},
	}

	for _, original := range tests {
		encoded, err := Encode(original)
		if err != nil {
			t.Fatalf("Encode falhou: %v", err)
		}

		decoded, err := Decode(original.Kind, encoded)
		if err != nil {
			t.Fatalf("Decode falhou: %v", err)
		}

		// vazio volta como nil: compara pelo tamanho
		if decoded.Ref != original.Ref ||
			decoded.TopicID != original.TopicID ||
			decoded.Publisher != original.Publisher ||
			len(decoded.Recipients) != len(original.Recipients) ||
			len(decoded.Payload) != len(original.Payload) {
			t.Errorf("registro diferente: esperado %+v, recebido %+v",
				original, decoded)
		}
	}
}

func TestDecodeTopicNameInvalidUTF8(t *testing.T) {
	data := []byte{
		1,    // TopicID
		2,    // Tamanho do nome
		0xff, // Byte UTF-8 inválido
		0xfe, // Byte UTF-8 inválido
	}

	_, err := Decode(KindTopicCreated, data)

	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("esperado ErrCorrupt, recebido: %v", err)
	}
}

// O payload decodificado é uma cópia: mexer no buffer original não o altera.
func TestDecodeCopiaPayload(t *testing.T) {
	encoded, _ := Encode(Record{Kind: KindPublished, Ref: 1, TopicID: 1, Recipients: []uint16{1}, Payload: []byte{9}})
	decoded, _ := Decode(KindPublished, encoded)

	encoded[len(encoded)-1] = 0

	if decoded.Payload[0] != 9 {
		t.Fatal("o payload aponta para o buffer de entrada")
	}
}

// Nenhum Data corrompido pode causar panic, e tudo que decodifica deve
// recodificar para os mesmos bytes.
// Rodar: go test ./broker/logrec -run x -fuzz FuzzDecode -fuzztime 15s
func FuzzDecode(f *testing.F) {
	f.Add(KindPublished, []byte{0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 1, 0, 9, 1, 7})
	f.Add(KindTopicCreated, []byte{1, 1, 'a'})

	f.Fuzz(func(t *testing.T, kind byte, d []byte) {
		r, err := Decode(kind, d)
		if err != nil {
			return
		}

		e, err := Encode(r)
		if err != nil || !bytes.Equal(e, d) {
			t.Fatalf("kind=%d %X recodifica como %X (%v)", kind, d, e, err)
		}
	})
}
