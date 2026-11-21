// Package logrec codifica e decodifica o campo Data dos registros que o
// broker grava no log (wal). O wal guarda {Kind, Data} sem interpretar Data.
//
//	Kind  Registro         Data
//	1     TOPIC_CREATED    topic_id(1) tamanho(1) nome
//	2     TOPIC_REMOVING   topic_id(1)
//	3     TOPIC_PURGED     topic_id(1)
//	4     SUBSCRIBED       topic_id(1) client_id(2)
//	5     UNSUBSCRIBED     topic_id(1) client_id(2)
//	6     PUBLISHED        ref(8) topic_id(1) flags(1) publicador(2)
//	                       n(2) destinatários(2·n) tamanho(1) payload
//	7     DELIVERED        ref(8) client_id(2)
//	8     QUARANTINED      ref(8) client_id(2)
//	9     EPOCH            época(4)
//
// Números com mais de um byte são big-endian.
package logrec

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

const (
	KindTopicCreated  byte = 1
	KindTopicRemoving byte = 2
	KindTopicPurged   byte = 3
	KindSubscribed    byte = 4
	KindUnsubscribed  byte = 5
	KindPublished     byte = 6
	KindDelivered     byte = 7
	KindQuarantined   byte = 8
	KindEpoch         byte = 9

	// Mesmos limites do protocolo, para os dois nunca divergirem.
	MaxTopicName = protocol.MaxTopicNameLen // 63
	MaxPayload   = protocol.MaxPayloadSize  // 249
)

var ErrCorrupt = errors.New("registro do log inválido")

// Record representa qualquer registro; cada Kind usa só alguns campos.
type Record struct {
	Kind      byte
	TopicID   uint8
	TopicName string
	Client    uint16
	Epoch     uint32

	Ref        uint64 // id interno da publicação no broker; nunca vai para a rede
	Flags      byte
	Publisher  uint16
	Recipients []uint16
	Payload    []byte
}

func validName(name string) bool {
	return len(name) > 0 && len(name) <= MaxTopicName && utf8.ValidString(name)
}

func Encode(r Record) ([]byte, error) {
	switch r.Kind {
	case KindTopicCreated:
		if r.TopicID == 0 || !validName(r.TopicName) {
			return nil, ErrCorrupt
		}

		b := []byte{r.TopicID, byte(len(r.TopicName))}
		return append(b, r.TopicName...), nil

	case KindTopicRemoving, KindTopicPurged:
		if r.TopicID == 0 {
			return nil, ErrCorrupt
		}

		return []byte{r.TopicID}, nil

	case KindSubscribed, KindUnsubscribed:
		if r.TopicID == 0 {
			return nil, ErrCorrupt
		}

		b := []byte{r.TopicID}
		b = binary.BigEndian.AppendUint16(b, r.Client)
		return b, nil

	case KindDelivered, KindQuarantined:
		b := make([]byte, 0, 10)
		b = binary.BigEndian.AppendUint64(b, r.Ref)
		b = binary.BigEndian.AppendUint16(b, r.Client)
		return b, nil

	case KindEpoch:
		b := make([]byte, 0, 4)
		b = binary.BigEndian.AppendUint32(b, r.Epoch)
		return b, nil

	case KindPublished:
		if r.TopicID == 0 ||
			len(r.Recipients) > 65535 ||
			len(r.Payload) > MaxPayload {
			return nil, ErrCorrupt
		}

		b := make([]byte, 0, 15+len(r.Recipients)*2+len(r.Payload))

		b = binary.BigEndian.AppendUint64(b, r.Ref)
		b = append(b, r.TopicID, r.Flags)
		b = binary.BigEndian.AppendUint16(b, r.Publisher)
		b = binary.BigEndian.AppendUint16(b, uint16(len(r.Recipients)))

		for _, recipient := range r.Recipients {
			b = binary.BigEndian.AppendUint16(b, recipient)
		}

		b = append(b, byte(len(r.Payload)))
		b = append(b, r.Payload...)

		return b, nil
	}

	return nil, ErrCorrupt
}

// Decode reconstrói o registro a partir do tipo e do Data lido do wal.
// Lista de destinatários e payload vazios voltam como nil (o broker só
// usa len e range, então nil e vazio se comportam igual).
func Decode(kind byte, d []byte) (Record, error) {
	r := Record{Kind: kind}

	switch kind {
	case KindTopicCreated:
		if len(d) < 2 || d[0] == 0 {
			return Record{}, ErrCorrupt
		}

		nameLen := int(d[1])

		if nameLen == 0 ||
			nameLen > MaxTopicName ||
			len(d) != 2+nameLen ||
			!utf8.Valid(d[2:]) {
			return Record{}, ErrCorrupt
		}

		r.TopicID = d[0]
		r.TopicName = string(d[2:])

	case KindTopicRemoving, KindTopicPurged:
		if len(d) != 1 || d[0] == 0 {
			return Record{}, ErrCorrupt
		}

		r.TopicID = d[0]

	case KindSubscribed, KindUnsubscribed:
		if len(d) != 3 || d[0] == 0 {
			return Record{}, ErrCorrupt
		}

		r.TopicID = d[0]
		r.Client = binary.BigEndian.Uint16(d[1:3])

	case KindDelivered, KindQuarantined:
		if len(d) != 10 {
			return Record{}, ErrCorrupt
		}

		r.Ref = binary.BigEndian.Uint64(d[0:8])
		r.Client = binary.BigEndian.Uint16(d[8:10])

	case KindEpoch:
		if len(d) != 4 {
			return Record{}, ErrCorrupt
		}

		r.Epoch = binary.BigEndian.Uint32(d)

	case KindPublished:
		// cabeçalho fixo: ref(8) + topic_id(1) + flags(1) + publicador(2) + n(2)
		if len(d) < 14 {
			return Record{}, ErrCorrupt
		}

		offset := 0

		r.Ref = binary.BigEndian.Uint64(d[offset : offset+8])
		offset += 8

		r.TopicID = d[offset]
		offset++

		if r.TopicID == 0 {
			return Record{}, ErrCorrupt
		}

		r.Flags = d[offset]
		offset++

		r.Publisher = binary.BigEndian.Uint16(d[offset : offset+2])
		offset += 2

		n := int(binary.BigEndian.Uint16(d[offset : offset+2]))
		offset += 2

		recipientBytes := n * 2

		// confere ANTES de ler: destinatários + o byte do tamanho do payload
		if len(d)-offset < recipientBytes+1 {
			return Record{}, ErrCorrupt
		}

		if n > 0 {
			r.Recipients = make([]uint16, n)

			for i := 0; i < n; i++ {
				r.Recipients[i] = binary.BigEndian.Uint16(d[offset : offset+2])
				offset += 2
			}
		}

		payloadLen := int(d[offset])
		offset++

		if payloadLen > MaxPayload ||
			len(d)-offset != payloadLen {
			return Record{}, ErrCorrupt
		}

		if payloadLen > 0 {
			// cópia: o wal pode reutilizar o buffer d depois
			r.Payload = append([]byte(nil), d[offset:]...)
		}

	default:
		return Record{}, ErrCorrupt
	}

	return r, nil
}
