package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/GabriellCarvallho/mare-pubsub/client/localqueue"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

var (
	ErrQueueFull = localqueue.ErrQueueFull
	ErrClosed    = errors.New("cliente fechado")
)

const (
	publisherQueueLimit = 10 << 20
	requestTimeout      = 5 * time.Second
	initialRetryDelay   = time.Second
	maxRetryDelay       = 60 * time.Second
)

// Config contém as opções comuns das bibliotecas cliente.
type Config struct {
	Brokers  []string
	ClientID uint16
	QueueDir string
	OnError  func(topic string, payload []byte, err error)
}

// Publisher grava as publicações localmente e as envia em segundo plano.
type Publisher interface {
	// Publish bloqueia somente até persistir a leitura na fila local.
	Publish(topic string, critical bool, payload []byte) error
	Close() error
}

// Subscriber será implementado na próxima etapa da biblioteca cliente.
type Subscriber interface {
	Subscribe(topic string, handle func(payload []byte) error) error
	Unsubscribe(topic string) error
	Close() error
}

// NewPublisher abre a fila local e inicia o envio em segundo plano.
// A conexão com um broker é estabelecida quando houver publicações pendentes.
func NewPublisher(cfg Config) (Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("informe ao menos um endereço de broker")
	}
	cfg.Brokers = append([]string(nil), cfg.Brokers...)
	if cfg.QueueDir == "" {
		return nil, errors.New("QueueDir não pode ser vazio")
	}
	for _, address := range cfg.Brokers {
		if strings.TrimSpace(address) == "" {
			return nil, errors.New("endereço de broker vazio")
		}
	}
	if err := os.MkdirAll(cfg.QueueDir, 0o700); err != nil {
		return nil, fmt.Errorf("criar diretório da fila: %w", err)
	}

	queuePath := filepath.Join(cfg.QueueDir, fmt.Sprintf("publisher-%d.queue", cfg.ClientID))
	queue, err := localqueue.Open(queuePath, publisherQueueLimit)
	if err != nil {
		return nil, fmt.Errorf("abrir fila local: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &publisher{
		cfg:      cfg,
		queue:    queue,
		ctx:      ctx,
		cancel:   cancel,
		signal:   make(chan struct{}, 1),
		topicIDs: make(map[string]uint8),
	}
	p.wg.Add(1)
	go p.run()
	return p, nil
}

type publisher struct {
	cfg    Config
	queue  localqueue.Queue
	ctx    context.Context
	cancel context.CancelFunc
	signal chan struct{}
	wg     sync.WaitGroup

	mu       sync.Mutex // protege closed e a entrada na fila durante Close
	closed   bool
	connMu   sync.Mutex // permite Close interromper uma conexão em uso
	conn     *wireConn
	maxEpoch uint32
	topicIDs map[string]uint8 // acessado somente pela goroutine run
}

type brokerError struct{ code byte }

func (e *brokerError) Error() string {
	return "broker retornou " + protocol.CodeName(e.code)
}

func (p *publisher) Publish(topic string, critical bool, payload []byte) error {
	if len(topic) == 0 || len(topic) > protocol.MaxTopicNameLen || !utf8.ValidString(topic) {
		return errors.New("nome de tópico inválido")
	}
	if len(payload) == 0 || len(payload) > protocol.MaxPayloadSize {
		return fmt.Errorf("payload deve ter entre 1 e %d bytes", protocol.MaxPayloadSize)
	}

	var flags byte
	if critical {
		flags = protocol.FlagCritical
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	_, err := p.queue.Append(topic, flags, payload)
	p.mu.Unlock()
	if err != nil {
		if errors.Is(err, localqueue.ErrQueueFull) {
			return ErrQueueFull
		}
		return err
	}

	select {
	case p.signal <- struct{}{}:
	default:
	}
	return nil
}

func (p *publisher) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cancel()
	p.mu.Unlock()

	p.connMu.Lock()
	conn := p.conn
	p.conn = nil
	p.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}

	p.wg.Wait()
	return p.queue.Close()
}

func (p *publisher) run() {
	defer p.wg.Done()
	delay := initialRetryDelay

	for {
		if p.ctx.Err() != nil {
			return
		}
		pending, err := p.queue.Pending()
		if err != nil {
			return
		}
		if len(pending) == 0 {
			select {
			case <-p.ctx.Done():
				return
			case <-p.signal:
				continue
			}
		}

		conn, err := p.ensureConnection()
		if err != nil {
			if !p.wait(delay) {
				return
			}
			delay = nextDelay(delay)
			continue
		}

		retry := false
		for _, entry := range pending {
			if p.ctx.Err() != nil {
				return
			}
			if err := p.publishEntry(conn, entry); err != nil {
				var brokerErr *brokerError
				if errors.As(err, &brokerErr) {
					switch {
					case protocol.IsTerminal(brokerErr.code):
						p.notifyError(entry, err)
						if removeErr := p.queue.Remove(entry.ID); removeErr != nil {
							return
						}
						continue
					case protocol.IsTransient(brokerErr.code):
						retry = true // mantém a conexão e tenta novamente com recuo
						break
					default:
						p.dropConnection(conn) // NOT_LEADER ou erro de protocolo
						retry = true
						break
					}
				} else {
					p.dropConnection(conn)
					retry = true
				}
				break
			}

			if err := p.queue.Remove(entry.ID); err != nil {
				return
			}
			delay = initialRetryDelay
		}

		if retry {
			if !p.wait(delay) {
				return
			}
			delay = nextDelay(delay)
		}
	}
}

func (p *publisher) ensureConnection() (*wireConn, error) {
	p.connMu.Lock()
	current := p.conn
	p.connMu.Unlock()
	if current != nil {
		return current, nil
	}

	var lastErr error
	for _, address := range p.cfg.Brokers {
		if p.ctx.Err() != nil {
			return nil, p.ctx.Err()
		}
		dialCtx, cancel := context.WithTimeout(p.ctx, requestTimeout)
		rawConn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		conn := newWireConn(rawConn)
		reply, err := p.exchange(conn, protocol.Frame{
			Type:     protocol.TypeHello,
			Version:  protocol.Version,
			ClientID: p.cfg.ClientID,
		})
		if err != nil {
			_ = conn.Close()
			lastErr = err
			continue
		}
		if reply.Type != protocol.TypeStatus {
			_ = conn.Close()
			lastErr = fmt.Errorf("resposta ao HELLO deveria ser STATUS, recebida %s", protocol.TypeName(reply.Type))
			continue
		}
		if reply.Role != protocol.RolePrincipal || reply.Epoch < p.maxEpoch {
			_ = conn.Close()
			lastErr = fmt.Errorf("broker %s não é o principal válido (papel=%d, época=%d)", address, reply.Role, reply.Epoch)
			continue
		}
		if reply.Epoch > p.maxEpoch {
			p.topicIDs = make(map[string]uint8)
			p.maxEpoch = reply.Epoch
		}

		p.connMu.Lock()
		if p.ctx.Err() != nil {
			p.connMu.Unlock()
			_ = conn.Close()
			return nil, p.ctx.Err()
		}
		p.conn = conn
		p.connMu.Unlock()
		return conn, nil
	}
	if lastErr == nil {
		lastErr = errors.New("nenhum broker principal disponível")
	}
	return nil, lastErr
}

func (p *publisher) publishEntry(conn *wireConn, entry localqueue.Entry) error {
	for attempt := 0; attempt < 2; attempt++ {
		topicID, err := p.resolveTopic(conn, entry.Topic)
		if err != nil {
			return err
		}
		reply, err := p.exchange(conn, protocol.Frame{
			Type:    protocol.TypePublish,
			Flags:   entry.Flags,
			TopicID: topicID,
			Payload: entry.Payload,
		})
		if err != nil {
			return err
		}
		switch reply.Type {
		case protocol.TypePublishAck:
			return nil
		case protocol.TypeError:
			if reply.ErrCode == protocol.CodeTopicNotFound && attempt == 0 {
				delete(p.topicIDs, entry.Topic)
				continue // o topic_id pode ter mudado; resolve novamente
			}
			return &brokerError{code: reply.ErrCode}
		default:
			return fmt.Errorf("resposta inesperada ao PUBLISH: %s", protocol.TypeName(reply.Type))
		}
	}
	return errors.New("não foi possível resolver o tópico para publicação")
}

func (p *publisher) resolveTopic(conn *wireConn, topic string) (uint8, error) {
	if id, ok := p.topicIDs[topic]; ok {
		return id, nil
	}

	reply, err := p.exchange(conn, protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: topic})
	if err != nil {
		return 0, err
	}
	if reply.Type == protocol.TypeTopicID {
		p.topicIDs[topic] = reply.TopicID
		return reply.TopicID, nil
	}
	if reply.Type != protocol.TypeError || reply.ErrCode != protocol.CodeTopicNotFound {
		if reply.Type == protocol.TypeError {
			return 0, &brokerError{code: reply.ErrCode}
		}
		return 0, fmt.Errorf("resposta inesperada ao RESOLVE_TOPIC: %s", protocol.TypeName(reply.Type))
	}

	reply, err = p.exchange(conn, protocol.Frame{Type: protocol.TypeCreateTopic, TopicName: topic})
	if err != nil {
		return 0, err
	}
	if reply.Type == protocol.TypeError {
		return 0, &brokerError{code: reply.ErrCode}
	}
	if reply.Type != protocol.TypeTopicID {
		return 0, fmt.Errorf("resposta inesperada ao CREATE_TOPIC: %s", protocol.TypeName(reply.Type))
	}
	p.topicIDs[topic] = reply.TopicID
	return reply.TopicID, nil
}

func (p *publisher) exchange(conn *wireConn, request protocol.Frame) (protocol.Frame, error) {
	if err := conn.send(request); err != nil {
		return protocol.Frame{}, err
	}
	ctx, cancel := context.WithTimeout(p.ctx, requestTimeout)
	defer cancel()
	reply, err := conn.receive(ctx)
	if err != nil {
		return protocol.Frame{}, err
	}
	if reply.Type == protocol.TypeError {
		return reply, nil
	}
	return reply, nil
}

func (p *publisher) dropConnection(conn *wireConn) {
	p.connMu.Lock()
	if p.conn == conn {
		p.conn = nil
	}
	p.connMu.Unlock()
	_ = conn.Close()
	p.topicIDs = make(map[string]uint8)
}

func (p *publisher) notifyError(entry localqueue.Entry, err error) {
	if p.cfg.OnError == nil {
		return
	}
	payload := append([]byte(nil), entry.Payload...)
	go p.cfg.OnError(entry.Topic, payload, err)
}

func (p *publisher) wait(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-p.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextDelay(current time.Duration) time.Duration {
	if current >= maxRetryDelay/2 {
		return maxRetryDelay
	}
	return current * 2
}
