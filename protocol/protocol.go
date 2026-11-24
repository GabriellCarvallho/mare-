// Package protocol implementa o protocolo binário do Maré Pub/Sub
// (documento de projeto, Seção 3).
//
// Quadro = prefixo uint16 big-endian com o tamanho do corpo + corpo.
// O corpo começa pelo tipo do comando (1 byte). Números multibyte são
// big-endian. PUBLISH e MESSAGE têm um cabeçalho do middleware seguido de
// um payload opaco, definido pela aplicação.
package protocol

import (
	"errors"
	"fmt"
)

// Erros do contrato (divisão de tarefas, Parte 1). Compare com errors.Is.
var (
	ErrInvalidFrame = errors.New("quadro inválido")     // tamanho, tipo ou corpo incompatível
	ErrFieldRange   = errors.New("valor fora do campo") // não cabe no tamanho definido
)

// Version é a versão do protocolo informada em HELLO.
const Version byte = 1

// Limites (Seções 3.1, 3.5 e 7.1).
const (
	LengthPrefixSize  = 2
	MaxBodySize       = 256
	PublishHeaderSize = 3 // tipo + flags + topic_id
	MessageHeaderSize = 7 // tipo + flags + topic_id + delivery_id
	MaxTopicNameLen   = 63
	// O payload é limitado pelo MESSAGE, que tem o cabeçalho maior: todo
	// PUBLISH aceito precisa poder ser entregue sem passar de 256 bytes.
	MaxPayloadSize = MaxBodySize - MessageHeaderSize // 249
)

// Tipos de comando. PUBLISH = 0x01 e MESSAGE = 0x02 vêm da tabela 3.2; os
// demais códigos são decisão de implementação, a combinar com o Felipe.
const (
	TypePublish     byte = 0x01 // flags, topic_id, payload
	TypeMessage     byte = 0x02 // flags, topic_id, delivery_id, payload
	TypePublishAck  byte = 0x03 // só o tipo; confirma pela ordem dos pedidos
	TypeDeliveryAck byte = 0x04 // delivery_id

	TypeHello   byte = 0x10 // versão, client_id
	TypeStatus  byte = 0x11 // papel, época
	TypePromote byte = 0x12 // só o tipo; operador → reserva (resposta: STATUS ou ERROR)

	TypeCreateTopic  byte = 0x20 // nome
	TypeResolveTopic byte = 0x21 // nome
	TypeTopicID      byte = 0x22 // topic_id (resposta a CREATE/RESOLVE)
	TypeDeleteTopic  byte = 0x23 // topic_id
	TypeSubscribe    byte = 0x24 // topic_id
	TypeUnsubscribe  byte = 0x25 // topic_id
	TypeOK           byte = 0x26 // só o tipo (resposta a DELETE/SUB/UNSUB)

	TypeError byte = 0x30 // código do erro
)

// Flags de PUBLISH/MESSAGE.
const (
	FlagCritical     byte = 1 << 0 // bit 0 = CRÍTICA; 0 = NORMAL
	flagReservedMask byte = 0xFE
)

// Papéis informados em STATUS.
const (
	RolePrincipal byte = 0x01
	RoleReserva   byte = 0x02
)

// Códigos de erro transportados em ERROR (Seção 3.6).
const (
	// Quadro rejeitado.
	CodeInvalidFrame       byte = 0x01
	CodeInvalidMessage     byte = 0x02
	CodeUnsupportedVersion byte = 0x03
	CodeHelloRequired      byte = 0x04
	// Terminais: a aplicação é informada; não insistir.
	CodeNoSubscribers byte = 0x10
	CodeTopicNotFound byte = 0x11
	CodeTopicRemoving byte = 0x12
	CodeTooManyTopics byte = 0x13
	// Transitórios: manter na fila local e tentar com recuo.
	CodeOverloaded         byte = 0x20
	CodeStorageUnavailable byte = 0x21
	CodeRecovering         byte = 0x22
	// Liderança: procurar o principal válido.
	CodeNotLeader        byte = 0x30
	CodeAlreadyPrincipal byte = 0x31 // PROMOTE enviado a quem já é principal
)

// Frame representa qualquer quadro. Cada tipo usa só alguns campos; os
// demais são ignorados por Encode e ficam zerados após a decodificação.
type Frame struct {
	Type       byte
	Flags      byte   // bit 0 = CRÍTICA (PUBLISH, MESSAGE)
	TopicID    uint8  // PUBLISH, MESSAGE, TOPIC_ID, DELETE/SUB/UNSUB
	DeliveryID uint32 // só em MESSAGE e DELIVERY_ACK
	ClientID   uint16 // só em HELLO
	Version    byte   // HELLO
	Role       byte   // STATUS
	Epoch      uint32 // STATUS
	TopicName  string // CREATE_TOPIC e RESOLVE_TOPIC
	ErrCode    byte   // ERROR
	Payload    []byte // PUBLISH e MESSAGE; opaco, definido pela aplicação
}

// Critical informa se o bit de prioridade está ligado.
func (f Frame) Critical() bool { return f.Flags&FlagCritical != 0 }

var typeNames = map[byte]string{
	TypePublish: "PUBLISH", TypeMessage: "MESSAGE",
	TypePublishAck: "PUBLISH_ACK", TypeDeliveryAck: "DELIVERY_ACK",
	TypeHello: "HELLO", TypeStatus: "STATUS", TypePromote: "PROMOTE",
	TypeCreateTopic: "CREATE_TOPIC", TypeResolveTopic: "RESOLVE_TOPIC",
	TypeTopicID: "TOPIC_ID", TypeDeleteTopic: "DELETE_TOPIC",
	TypeSubscribe: "SUBSCRIBE", TypeUnsubscribe: "UNSUBSCRIBE",
	TypeOK: "OK", TypeError: "ERROR",
}

var codeNames = map[byte]string{
	CodeInvalidFrame: "INVALID_FRAME", CodeInvalidMessage: "INVALID_MESSAGE",
	CodeUnsupportedVersion: "UNSUPPORTED_VERSION", CodeHelloRequired: "HELLO_REQUIRED",
	CodeNoSubscribers: "NO_SUBSCRIBERS", CodeTopicNotFound: "TOPIC_NOT_FOUND",
	CodeTopicRemoving: "TOPIC_REMOVING", CodeTooManyTopics: "TOO_MANY_TOPICS",
	CodeOverloaded: "OVERLOADED", CodeStorageUnavailable: "STORAGE_UNAVAILABLE",
	CodeRecovering: "RECOVERING", CodeNotLeader: "NOT_LEADER",
	CodeAlreadyPrincipal: "ALREADY_PRINCIPAL",
}

// TypeName devolve o nome do comando, por exemplo "PUBLISH".
func TypeName(t byte) string {
	if s, ok := typeNames[t]; ok {
		return s
	}
	return fmt.Sprintf("TIPO_DESCONHECIDO(0x%02X)", t)
}

// CodeName devolve o nome do código de erro, por exemplo "NO_SUBSCRIBERS".
func CodeName(c byte) string {
	if s, ok := codeNames[c]; ok {
		return s
	}
	return fmt.Sprintf("CODIGO_DESCONHECIDO(0x%02X)", c)
}

// IsTerminal: não repetir a mesma publicação (Seção 3.6).
func IsTerminal(c byte) bool {
	return c == CodeNoSubscribers || c == CodeTopicNotFound ||
		c == CodeTopicRemoving || c == CodeTooManyTopics
}

// IsTransient: conservar na fila local e tentar novamente com recuo.
func IsTransient(c byte) bool {
	return c == CodeOverloaded || c == CodeStorageUnavailable || c == CodeRecovering
}

// String descreve o quadro em texto (usado pelo maredump e em logs).
func (f Frame) String() string {
	name := TypeName(f.Type)
	prio := "NORMAL"
	if f.Critical() {
		prio = "CRÍTICA"
	}
	switch f.Type {
	case TypePublish:
		return fmt.Sprintf("%s prioridade=%s topic_id=%d payload=%d bytes [% X]",
			name, prio, f.TopicID, len(f.Payload), f.Payload)
	case TypeMessage:
		return fmt.Sprintf("%s prioridade=%s topic_id=%d delivery_id=%d payload=%d bytes [% X]",
			name, prio, f.TopicID, f.DeliveryID, len(f.Payload), f.Payload)
	case TypeDeliveryAck:
		return fmt.Sprintf("%s delivery_id=%d", name, f.DeliveryID)
	case TypeHello:
		return fmt.Sprintf("%s versão=%d client_id=%d", name, f.Version, f.ClientID)
	case TypeStatus:
		role := "principal"
		if f.Role == RoleReserva {
			role = "reserva"
		}
		return fmt.Sprintf("%s papel=%s época=%d", name, role, f.Epoch)
	case TypeCreateTopic, TypeResolveTopic:
		return fmt.Sprintf("%s nome=%q", name, f.TopicName)
	case TypeTopicID, TypeDeleteTopic, TypeSubscribe, TypeUnsubscribe:
		return fmt.Sprintf("%s topic_id=%d", name, f.TopicID)
	case TypeError:
		return fmt.Sprintf("%s código=%s", name, CodeName(f.ErrCode))
	}
	return name
}
