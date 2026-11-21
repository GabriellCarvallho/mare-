// Package server é o broker do Maré Pub/Sub.
//
// Passo 3 (este arquivo): servidor TCP básico. Cada conexão tem duas
// goroutines:
//
//   - a LEITORA lê um quadro por vez, processa e enfileira a resposta. Como
//     ela trata os pedidos em ordem, as respostas saem na ordem dos pedidos
//     (Seção 3.4 do documento).
//   - a ESCRITORA é a única que escreve no socket. Ela tira quadros da fila
//     `out` e escreve. Assim duas goroutines nunca escrevem ao mesmo tempo
//     (o contrato de protocol.WriteFrame exige isso).
//
// Passo 4 (topics.go): comandos de tópicos e assinaturas pela rede.
//
// Os próximos passos acrescentam publicação e entrega (5), log (6) e
// reserva/promoção (7).
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/topics"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

const (
	RolePrincipal = "principal"
	RoleReserva   = "reserva"
)

type Config struct {
	Addr     string        // ex.: ":7000"
	Role     string        // "principal" ou "reserva"
	DataDir  string        // onde ficam o log e a época (usado no passo 6)
	Recovery time.Duration // janela RECOVERING, 30 s (usado no passo 7)

	// Extensões para testes. Valores zero usam o padrão.
	OnListen func(net.Addr)                   // recebe o endereço real (útil com ":0")
	Logf     func(format string, args ...any) // padrão: log.Printf
}

// broker guarda o estado compartilhado. Tudo que várias goroutines leem ou
// escrevem fica protegido por mu.
type broker struct {
	cfg Config

	mu       sync.Mutex
	role     byte
	epoch    uint32
	sessions map[*session]struct{} // conexões abertas

	topics interface {
		topics.Registry
		topics.Restorer
	} // Parte 2
	removing map[uint8]bool // tópicos EM_REMOÇÃO

	wg sync.WaitGroup // espera as goroutines no encerramento
}

// session é uma conexão TCP com um cliente.
type session struct {
	b          *broker
	conn       net.Conn
	out        chan protocol.Frame // fila da escritora
	done       chan struct{}       // fechado quando a leitora termina
	writerDone chan struct{}       // fechado quando a escritora termina

	hello    bool   // já recebeu HELLO?
	clientID uint16 // client_id informado no HELLO
}

// closeAfterWrite é um quadro "sentinela": a escritora fecha a conexão depois
// de escrever tudo o que veio antes dele. Type 0 não existe no protocolo.
var closeAfterWrite = protocol.Frame{Type: 0}

// Run bloqueia até ctx ser cancelado ou ocorrer um erro fatal (porta em uso).
// Ao cancelar: para de aceitar conexões, fecha as abertas e espera as
// goroutines terminarem.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Role != RolePrincipal && cfg.Role != RoleReserva {
		return fmt.Errorf("papel inválido %q (use %q ou %q)", cfg.Role, RolePrincipal, RoleReserva)
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}

	b := &broker{
		cfg:      cfg,
		role:     protocol.RoleReserva,
		sessions: make(map[*session]struct{}),
		topics:   topics.New(),
		removing: make(map[uint8]bool),
	}
	if cfg.Role == RolePrincipal {
		b.role = protocol.RolePrincipal
		b.epoch = 1 // provisório: o passo 7 cuida da época de verdade
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	if cfg.OnListen != nil {
		cfg.OnListen(ln.Addr())
	}
	cfg.Logf("broker %s ouvindo em %s", cfg.Role, ln.Addr())

	b.wg.Add(1)
	go b.acceptLoop(ln)

	<-ctx.Done() // espera o pedido de encerramento

	ln.Close() // acceptLoop sai
	b.mu.Lock()
	for s := range b.sessions {
		s.conn.Close() // as leitoras saem com erro de leitura
	}
	b.mu.Unlock()
	b.wg.Wait()
	return nil
}

func (b *broker) acceptLoop(ln net.Listener) {
	defer b.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener fechado: encerramento
		}
		s := &session{
			b:          b,
			conn:       conn,
			out:        make(chan protocol.Frame, 256),
			done:       make(chan struct{}),
			writerDone: make(chan struct{}),
		}
		b.mu.Lock()
		b.sessions[s] = struct{}{}
		b.mu.Unlock()

		b.wg.Add(2)
		go s.writeLoop()
		go s.readLoop()
	}
}

// writeLoop é a ÚNICA goroutine que escreve nesta conexão.
func (s *session) writeLoop() {
	defer s.b.wg.Done()
	defer close(s.writerDone)
	for {
		select {
		case f := <-s.out:
			if f.Type == closeAfterWrite.Type {
				s.conn.Close()
				return
			}
			// Cliente travado não segura o broker para sempre.
			s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := protocol.WriteFrame(s.conn, f); err != nil {
				s.conn.Close() // a leitora vai perceber e encerrar
				return
			}
		case <-s.done:
			return
		}
	}
}

// reply enfileira uma resposta. Se a fila estiver cheia, espera: isso só
// freia este cliente, nunca os outros.
func (s *session) reply(f protocol.Frame) {
	select {
	case s.out <- f:
	case <-s.done:
	case <-s.writerDone:
	}
}

func errFrame(code byte) protocol.Frame {
	return protocol.Frame{Type: protocol.TypeError, ErrCode: code}
}

// readLoop lê e processa os pedidos, um por vez, em ordem.
func (s *session) readLoop() {
	defer s.b.wg.Done()

	var invalid bool
	for {
		f, err := protocol.ReadFrame(s.conn)
		if err != nil {
			// io.EOF = cliente fechou; ErrInvalidFrame = quadro malformado;
			// qualquer outro = conexão caiu. Em todos, esta conexão acaba.
			invalid = errors.Is(err, protocol.ErrInvalidFrame)
			break
		}
		s.reply(s.handle(f))
	}

	if invalid {
		// Avisa o cliente e fecha só esta conexão (Seção 3.6): as outras
		// seguem normais.
		s.reply(errFrame(protocol.CodeInvalidFrame))
		s.reply(closeAfterWrite)
		select {
		case <-s.writerDone:
		case <-time.After(5 * time.Second):
		}
	}

	close(s.done)
	s.conn.Close()
	<-s.writerDone

	s.b.mu.Lock()
	delete(s.b.sessions, s)
	s.b.mu.Unlock()
}

// handle decide a resposta de cada pedido.
func (s *session) handle(f protocol.Frame) protocol.Frame {
	if f.Type == protocol.TypeHello {
		return s.handleHello(f)
	}
	if !s.hello {
		return errFrame(protocol.CodeHelloRequired)
	}

	// A reserva não atende nada além do HELLO até ser promovida (Seção 6.1).
	s.b.mu.Lock()
	standby := s.b.role != protocol.RolePrincipal
	s.b.mu.Unlock()
	if standby {
		return errFrame(protocol.CodeNotLeader)
	}

	switch f.Type {
	case protocol.TypeCreateTopic:
		return s.b.createTopic(f.TopicName)
	case protocol.TypeResolveTopic:
		return s.b.resolveTopic(f.TopicName)
	case protocol.TypeDeleteTopic:
		return s.b.deleteTopic(f.TopicID)
	case protocol.TypeSubscribe:
		return s.b.subscribe(s, f.TopicID)
	case protocol.TypeUnsubscribe:
		return s.b.unsubscribe(s, f.TopicID)
		// Passo 5: PUBLISH e DELIVERY_ACK
	}

	// Por enquanto, qualquer outro comando é recusado.
	return errFrame(protocol.CodeInvalidMessage)
}

func (s *session) handleHello(f protocol.Frame) protocol.Frame {
	if f.Version != protocol.Version {
		return errFrame(protocol.CodeUnsupportedVersion)
	}
	s.hello = true
	s.clientID = f.ClientID

	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	return protocol.Frame{Type: protocol.TypeStatus, Role: s.b.role, Epoch: s.b.epoch}
}
