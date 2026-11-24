package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"unicode/utf8"
)

func rangeErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrFieldRange, fmt.Sprintf(format, a...))
}

func invalidErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidFrame, fmt.Sprintf(format, a...))
}

// ---------------------------------------------------------------------------
// Codificação

// Encode devolve o quadro completo (prefixo + corpo).
// Não tem estado: pode ser chamada de qualquer goroutine.
// Erros: ErrFieldRange (valor não cabe no campo), ErrInvalidFrame (tipo
// desconhecido ou corpo acima de 256 bytes).
func Encode(f Frame) ([]byte, error) {
	return AppendEncode(nil, f)
}

// AppendEncode faz o mesmo que Encode, mas acrescenta o quadro ao fim de dst.
// Reutilizando um buffer (buf[:0]) a codificação não aloca memória.
func AppendEncode(dst []byte, f Frame) ([]byte, error) {
	start := len(dst)
	dst = append(dst, 0, 0) // reserva o prefixo; preenchido no fim
	dst, err := appendBody(dst, f)
	if err != nil {
		return dst[:start], err
	}
	n := len(dst) - start - LengthPrefixSize
	if n > MaxBodySize {
		return dst[:start], invalidErr("corpo com %d bytes (máx. %d)", n, MaxBodySize)
	}
	binary.BigEndian.PutUint16(dst[start:], uint16(n))
	return dst, nil
}

func appendBody(b []byte, f Frame) ([]byte, error) {
	switch f.Type {
	case TypePublish, TypeMessage:
		if err := checkData(f); err != nil {
			return b, err
		}
		b = append(b, f.Type, f.Flags, f.TopicID)
		if f.Type == TypeMessage {
			b = binary.BigEndian.AppendUint32(b, f.DeliveryID)
		}
		return append(b, f.Payload...), nil

	case TypePublishAck, TypeOK:
		return append(b, f.Type), nil

	case TypeDeliveryAck:
		return binary.BigEndian.AppendUint32(append(b, f.Type), f.DeliveryID), nil

	case TypeHello:
		return binary.BigEndian.AppendUint16(append(b, f.Type, f.Version), f.ClientID), nil

	case TypeStatus:
		if err := checkRole(f.Role); err != nil {
			return b, err
		}
		return binary.BigEndian.AppendUint32(append(b, f.Type, f.Role), f.Epoch), nil

	case TypeCreateTopic, TypeResolveTopic:
		if err := checkName(f.TopicName); err != nil {
			return b, err
		}
		b = append(b, f.Type, byte(len(f.TopicName)))
		return append(b, f.TopicName...), nil

	case TypeTopicID, TypeDeleteTopic, TypeSubscribe, TypeUnsubscribe:
		if err := checkTopicID(f.TopicID); err != nil {
			return b, err
		}
		return append(b, f.Type, f.TopicID), nil

	case TypeError:
		if _, ok := codeNames[f.ErrCode]; !ok {
			return b, rangeErr("código de erro desconhecido 0x%02X", f.ErrCode)
		}
		return append(b, f.Type, f.ErrCode), nil
	}
	return b, invalidErr("tipo desconhecido 0x%02X", f.Type)
}

// ---------------------------------------------------------------------------
// Validações compartilhadas por Encode e Decode

func checkTopicID(id uint8) error {
	if id == 0 {
		return rangeErr("topic_id 0 (válidos: 1 a 255)")
	}
	return nil
}

func checkRole(r byte) error {
	if r != RolePrincipal && r != RoleReserva {
		return rangeErr("papel desconhecido 0x%02X", r)
	}
	return nil
}

func checkName(name string) error {
	if len(name) == 0 || len(name) > MaxTopicNameLen {
		return rangeErr("nome de tópico com %d bytes (1 a %d)", len(name), MaxTopicNameLen)
	}
	if !utf8.ValidString(name) {
		return rangeErr("nome de tópico não é UTF-8 válido")
	}
	return nil
}

// checkData valida o que PUBLISH e MESSAGE têm em comum.
func checkData(f Frame) error {
	if f.Flags&flagReservedMask != 0 {
		return rangeErr("flags reservadas ligadas: 0x%02X", f.Flags)
	}
	if err := checkTopicID(f.TopicID); err != nil {
		return err
	}
	if len(f.Payload) == 0 {
		return rangeErr("payload vazio")
	}
	if len(f.Payload) > MaxPayloadSize {
		return rangeErr("payload com %d bytes (máx. %d)", len(f.Payload), MaxPayloadSize)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Decodificação

// bodySize é o tamanho exato do corpo dos comandos de tamanho fixo.
var bodySize = map[byte]int{
	TypePublishAck: 1, TypeOK: 1,
	TypeDeliveryAck: 5,
	TypeHello:       4,
	TypeStatus:      6,
	TypeTopicID:     2, TypeDeleteTopic: 2, TypeSubscribe: 2, TypeUnsubscribe: 2,
	TypeError: 2,
}

// Decode interpreta um corpo (sem o prefixo). Qualquer violação do formato
// devolve um erro que satisfaz errors.Is(err, ErrInvalidFrame).
// Payload aponta para dentro de body, sem cópia.
func Decode(body []byte) (Frame, error) {
	f, err := decode(body)
	if err != nil {
		// Do lado de quem recebe, todo campo fora do formato é quadro inválido.
		return Frame{}, fmt.Errorf("%w: %w", ErrInvalidFrame, err)
	}
	return f, nil
}

func decode(b []byte) (Frame, error) {
	if len(b) == 0 {
		return Frame{}, fmt.Errorf("corpo vazio")
	}
	f := Frame{Type: b[0]}
	if want, ok := bodySize[f.Type]; ok && len(b) != want {
		return Frame{}, fmt.Errorf("%s com %d bytes de corpo (esperado %d)", TypeName(f.Type), len(b), want)
	}

	switch f.Type {
	case TypePublish, TypeMessage:
		hdr := PublishHeaderSize
		if f.Type == TypeMessage {
			hdr = MessageHeaderSize
		}
		if len(b) < hdr {
			return Frame{}, fmt.Errorf("%s com %d bytes (cabeçalho tem %d)", TypeName(f.Type), len(b), hdr)
		}
		f.Flags, f.TopicID = b[1], b[2]
		if f.Type == TypeMessage {
			f.DeliveryID = binary.BigEndian.Uint32(b[3:])
		}
		f.Payload = b[hdr:]
		return f, checkData(f)

	case TypePublishAck, TypeOK:
		return f, nil

	case TypeDeliveryAck:
		f.DeliveryID = binary.BigEndian.Uint32(b[1:])
		return f, nil

	case TypeHello:
		f.Version = b[1]
		f.ClientID = binary.BigEndian.Uint16(b[2:])
		return f, nil

	case TypeStatus:
		f.Role = b[1]
		f.Epoch = binary.BigEndian.Uint32(b[2:])
		return f, checkRole(f.Role)

	case TypeCreateTopic, TypeResolveTopic:
		if len(b) < 2 || len(b) != 2+int(b[1]) {
			return Frame{}, fmt.Errorf("%s: comprimento do nome não confere com o corpo", TypeName(f.Type))
		}
		f.TopicName = string(b[2:])
		return f, checkName(f.TopicName)

	case TypeTopicID, TypeDeleteTopic, TypeSubscribe, TypeUnsubscribe:
		f.TopicID = b[1]
		return f, checkTopicID(f.TopicID)

	case TypeError:
		f.ErrCode = b[1]
		if _, ok := codeNames[f.ErrCode]; !ok {
			return Frame{}, fmt.Errorf("código de erro desconhecido 0x%02X", f.ErrCode)
		}
		return f, nil
	}
	return Frame{}, fmt.Errorf("tipo desconhecido 0x%02X", f.Type)
}

// ---------------------------------------------------------------------------
// Leitura e escrita sobre a conexão

// ReadFrame bloqueia até ler um quadro inteiro ou a conexão falhar.
// TCP é um fluxo de bytes: io.ReadFull repete a leitura até completar.
// Erros: io.EOF (conexão fechada entre quadros), io.ErrUnexpectedEOF
// (fechada no meio de um quadro), ErrInvalidFrame. Depois de qualquer
// erro, quem chama fecha a conexão.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [LengthPrefixSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err // io.EOF ou io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint16(hdr[:]))
	// Valida o tamanho ANTES de alocar memória.
	if n == 0 || n > MaxBodySize {
		return Frame{}, invalidErr("corpo com %d bytes (1 a %d)", n, MaxBodySize)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF // o prefixo já foi lido: queda no meio
		}
		return Frame{}, err
	}
	return Decode(body)
}

// WriteFrame bloqueia até escrever o quadro inteiro. Não é segura para
// duas goroutines na mesma conexão: quem chama mantém uma única
// goroutine escritora por conexão.
func WriteFrame(w io.Writer, f Frame) error {
	b, err := Encode(f)
	if err != nil {
		return err
	}
	_, err = w.Write(b) // io.Writer garante: ou escreve tudo, ou devolve erro
	return err
}
