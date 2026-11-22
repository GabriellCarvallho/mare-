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
// Passo 5 (publish.go): PUBLISH → escalonador → MESSAGE → DELIVERY_ACK, com
// duas goroutines de fundo (despachante e relógio de reenvio).
//
// Passo 6 (log.go, persist.go): grava tudo no log antes de confirmar e
// reconstrói o estado ao reiniciar.
//
// O próximo passo acrescenta a reserva e a promoção (7).
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/delivery"
	"github.com/GabriellCarvallho/mare-pubsub/broker/logrec"
	"github.com/GabriellCarvallho/mare-pubsub/broker/sched"
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
	DataDir  string        // onde ficam o log e a época
	Recovery time.Duration // janela RECOVERING, 30 s (usado no passo 7)

	// Extensões. Valores zero usam o padrão.
	OnListen      func(net.Addr)                   // recebe o endereço real (útil com ":0")
	Logf          func(format string, args ...any) // padrão: log.Printf
	Sched         sched.Config                     // cotas (padrão: 1.000 por sensor)
	RetryInterval time.Duration                    // intervalo do relógio de reenvio (padrão 250 ms)
	Now           func() time.Time                 // relógio (padrão time.Now; os testes trocam)
	OpenLog       func(path string) (Log, error)   // abre o log (padrão: em memória, sem persistência)
	CompactEvery  int                              // compacta a cada N publicações concluídas (padrão 1.000)
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

	// Passo 5
	clients map[uint16][]*session   // client_id → conexões abertas (roteamento dos MESSAGE)
	sched   sched.Scheduler         // Parte 2: ordem 3:1 e cotas
	dm      *delivery.Manager       // passo 2: estado de cada entrega
	pubs    map[uint64]*publication // publicações ainda não concluídas
	nextRef uint64                  // próxima ref de publicação
	now     func() time.Time

	// Passo 6
	log        Log        // log da época atual
	logRecords int        // registros lidos na reconstrução (0 = log novo)
	completed  int        // publicações concluídas desde a última compactação
	fatal      chan error // erro que encerra o broker (log quebrado)

	wg sync.WaitGroup // espera as goroutines no encerramento
}

// session é uma conexão TCP com um cliente.
type session struct {
	b          *broker
	conn       net.Conn
	out        chan protocol.Frame // fila da escritora
	done       chan struct{}       // fechado quando a leitora termina
	writerDone chan struct{}       // fechado quando a escritora termina

	hello      bool   // já recebeu HELLO?
	clientID   uint16 // client_id informado no HELLO
	subscriber bool   // fez SUBSCRIBE nesta conexão: é a preferida para os MESSAGE
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
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 250 * time.Millisecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.OpenLog == nil {
		// Sem o wal do Felipe: log em memória, novo a cada execução.
		cfg.OpenLog = NewMemoryStore(0).Open
	}
	if cfg.CompactEvery <= 0 {
		cfg.CompactEvery = 1000
	}

	b := &broker{
		cfg:      cfg,
		role:     protocol.RoleReserva,
		sessions: make(map[*session]struct{}),
		topics:   topics.New(),
		removing: make(map[uint8]bool),
		clients:  make(map[uint16][]*session),
		sched:    sched.New(cfg.Sched),
		dm:       delivery.New(),
		pubs:     make(map[uint64]*publication),
		nextRef:  1,
		now:      cfg.Now,
		fatal:    make(chan error, 1),
	}
	if cfg.Role == RolePrincipal {
		b.role = protocol.RolePrincipal
		b.epoch = 1 // provisório: o passo 7 cuida da época de verdade

		// Passo 6: relê o log ANTES de aceitar conexões.
		if err := b.openLog(); err != nil {
			return err
		}
		if b.logRecords == 0 { // log novo: registra a época
			if err := b.appendRec(logrec.Record{Kind: logrec.KindEpoch, Epoch: b.epoch}); err != nil {
				b.log.Close()
				return err
			}
		}
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		b.closeLog()
		return err
	}
	if cfg.OnListen != nil {
		cfg.OnListen(ln.Addr())
	}
	cfg.Logf("broker %s ouvindo em %s", cfg.Role, ln.Addr())

	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()

	b.wg.Add(3)
	go b.acceptLoop(ln)
	go b.dispatchLoop(loopCtx) // passo 5: escalonador → entregas
	go b.retryLoop(loopCtx)    // passo 5: relógio de reenvio

	// Espera o pedido de encerramento ou um erro fatal (log quebrado).
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-b.fatal:
		cfg.Logf("erro fatal: %v", runErr)
	}

	stopLoops() // despachante e relógio saem
	ln.Close()  // acceptLoop sai
	b.mu.Lock()
	for s := range b.sessions {
		s.conn.Close() // as leitoras saem com erro de leitura
	}
	b.mu.Unlock()
	b.wg.Wait()
	b.closeLog() // o que foi confirmado continua no log
	return runErr
}

func (b *broker) closeLog() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.log != nil {
		b.log.Close()
		b.log = nil
	}
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
		if resp, hasReply := s.handle(f); hasReply {
			s.reply(resp)
		}
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

	s.b.unregister(s)
}

// unregister tira a conexão dos mapas. As entregas em voo do cliente voltam
// a aguardar, sem contar falha (Seção 4.2): se ele tiver outra conexão
// aberta (ou reconectar), elas são reenviadas por ela.
func (b *broker) unregister(s *session) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.sessions, s)
	if !s.hello {
		return
	}
	list := b.clients[s.clientID]
	for i, x := range list {
		if x == s {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(b.clients, s.clientID)
	} else {
		b.clients[s.clientID] = list
	}

	// O que foi enviado por esta conexão pode não ter chegado. Mesmo que o
	// cliente tenha outra conexão aberta, devolvemos tudo a AGUARDANDO; no
	// pior caso, ele recebe uma repetição (permitido pelo "pelo menos uma vez").
	b.dm.Disconnected(s.clientID)
	if len(list) > 0 {
		b.kick() // reenvia já pela outra conexão
	}
}

// handle decide a resposta de cada pedido. hasReply é false para o
// DELIVERY_ACK, que não tem resposta no protocolo.
func (s *session) handle(f protocol.Frame) (resp protocol.Frame, hasReply bool) {
	if f.Type == protocol.TypeHello {
		return s.handleHello(f)
	}
	if !s.hello {
		return errFrame(protocol.CodeHelloRequired), true
	}

	// A reserva não atende nada além do HELLO até ser promovida (Seção 6.1).
	s.b.mu.Lock()
	standby := s.b.role != protocol.RolePrincipal
	s.b.mu.Unlock()
	if standby {
		if f.Type == protocol.TypeDeliveryAck {
			return protocol.Frame{}, false
		}
		return errFrame(protocol.CodeNotLeader), true
	}

	switch f.Type {
	case protocol.TypeCreateTopic:
		return s.b.createTopic(f.TopicName), true
	case protocol.TypeResolveTopic:
		return s.b.resolveTopic(f.TopicName), true
	case protocol.TypeDeleteTopic:
		return s.b.deleteTopic(f.TopicID), true
	case protocol.TypeSubscribe:
		return s.b.subscribe(s, f.TopicID), true
	case protocol.TypeUnsubscribe:
		return s.b.unsubscribe(s, f.TopicID), true
	case protocol.TypePublish:
		return s.b.publish(s, f), true
	case protocol.TypeDeliveryAck:
		s.b.ack(s, f.DeliveryID)
		return protocol.Frame{}, false
	}

	// Tipos que só o broker envia (MESSAGE, STATUS, OK...) não são pedidos.
	return errFrame(protocol.CodeInvalidMessage), true
}

// handleHello registra o cliente e responde STATUS. O STATUS é enfileirado
// ainda com b.mu travado: assim nenhum MESSAGE para este cliente (do
// despachante ou do relógio) passa na frente dele.
func (s *session) handleHello(f protocol.Frame) (protocol.Frame, bool) {
	if f.Version != protocol.Version {
		return errFrame(protocol.CodeUnsupportedVersion), true
	}

	b := s.b
	b.mu.Lock()
	if !s.hello {
		s.hello = true
		s.clientID = f.ClientID
		b.clients[s.clientID] = append(b.clients[s.clientID], s)
	}
	status := protocol.Frame{Type: protocol.TypeStatus, Role: b.role, Epoch: b.epoch}
	sent := s.trySend(status)
	b.mu.Unlock()

	if !sent {
		s.reply(status) // fila cheia (raro): espera vaga
	}
	b.kick() // se o cliente voltou, recebe já o que estava pendente
	return protocol.Frame{}, false
}
