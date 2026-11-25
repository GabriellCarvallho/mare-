package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

type delivery struct {
	conn    *wireConn
	id      uint32
	payload []byte
}

type subscription struct {
	topic   string
	topicID uint8
	queue   chan delivery
	done    chan struct{}
	once    sync.Once

	// queueMu mantém a ordem entre mensagens recebidas antes do registro do
	// topic_id e mensagens que chegam enquanto o backlog é transferido.
	queueMu sync.Mutex

	handlerMu sync.RWMutex
	handler   func([]byte) error
}

func (s *subscription) setHandler(handler func([]byte) error) {
	s.handlerMu.Lock()
	s.handler = handler
	s.handlerMu.Unlock()
}

func (s *subscription) getHandler() func([]byte) error {
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	return s.handler
}

func (s *subscription) stop() {
	s.once.Do(func() { close(s.done) })
}

type subscriberConn struct {
	wire      *wireConn
	responses chan protocol.Frame
}

type subscriber struct {
	cfg              Config
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup

	// opMu serializa Subscribe, Unsubscribe e a recuperação da conexão.
	opMu             sync.Mutex
	mu               sync.RWMutex
	closed           bool
	active           *subscriberConn
	maxEpoch         uint32
	subs             map[string]*subscription
	byTopicID        map[uint8]*subscription

	// O broker pode reenviar entregas logo após HELLO, antes de a aplicação
	// chamar Subscribe e resolver os topic_ids. Mantemos essas mensagens até
	// que o mapeamento local seja reconstruído.
	pendingByTopicID map[uint8][]delivery
	reconnect        chan struct{}
}

// NewSubscriber cria um assinante. A conexão é aberta quando Subscribe é chamado.
func NewSubscriber(cfg Config) (Subscriber, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("informe ao menos um endereço de broker")
	}
	cfg.Brokers = append([]string(nil), cfg.Brokers...)
	for _, address := range cfg.Brokers {
		if strings.TrimSpace(address) == "" {
			return nil, errors.New("endereço de broker vazio")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &subscriber{
		cfg:              cfg,
		ctx:              ctx,
		cancel:           cancel,
		subs:             make(map[string]*subscription),
		byTopicID:        make(map[uint8]*subscription),
		pendingByTopicID: make(map[uint8][]delivery),
		reconnect:        make(chan struct{}, 1),
	}
	s.wg.Add(1)
	go s.reconnectLoop()
	return s, nil
}

func (s *subscriber) Subscribe(topic string, handler func(payload []byte) error) error {
	if topic == "" || len(topic) > protocol.MaxTopicNameLen || !utf8.ValidString(topic) {
		return errors.New("nome de tópico inválido")
	}
	if handler == nil {
		return errors.New("handler não pode ser nil")
	}

	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	if existing := s.subs[topic]; existing != nil {
		existing.setHandler(handler)
		return nil
	}

	conn, err := s.ensureConnectionLocked()
	if err != nil {
		return err
	}
	id, err := s.resolveTopic(conn, topic)
	if err != nil {
		s.handleCommandError(conn, err)
		return err
	}

	sub := &subscription{
		topic:   topic,
		topicID: id,
		queue:   make(chan delivery, 64),
		done:    make(chan struct{}),
		handler: handler,
	}

	// Registra antes de SUBSCRIBE: o broker pode enviar MESSAGE logo após
	// processar o comando, inclusive antes de a resposta OK ser lida.
	s.mu.Lock()
	s.subs[topic] = sub
	s.mu.Unlock()
	// Registra o topic_id e inicia o worker antes de SUBSCRIBE: mensagens
	// antigas podem chegar logo após HELLO, e novas logo após o SUBSCRIBE.
	s.registerSubscriptionID(sub, id, true)

	reply, err := s.exchange(conn, protocol.Frame{Type: protocol.TypeSubscribe, TopicID: id})
	if err != nil || reply.Type != protocol.TypeOK {
		s.removeSubscription(sub)
		sub.stop()
		if err == nil {
			err = responseError(reply, "SUBSCRIBE")
		}
		s.handleCommandError(conn, err)
		return err
	}

	return nil
}

func (s *subscriber) Unsubscribe(topic string) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	sub := s.subs[topic]
	if sub == nil {
		return nil
	}

	conn := s.currentConnection()
	if conn != nil {
		reply, err := s.exchange(conn, protocol.Frame{
			Type:    protocol.TypeUnsubscribe,
			TopicID: sub.topicID,
		})
		if err != nil {
			s.handleCommandError(conn, err)
			return err
		}
		if reply.Type != protocol.TypeOK {
			return responseError(reply, "UNSUBSCRIBE")
		}
	}

	s.removeSubscription(sub)
	sub.stop()
	return nil
}

func (s *subscriber) Close() error {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	conn := s.active
	s.active = nil
	subs := make([]*subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.subs = make(map[string]*subscription)
	s.byTopicID = make(map[uint8]*subscription)
	s.pendingByTopicID = make(map[uint8][]delivery)
	s.mu.Unlock()

	if conn != nil {
		_ = conn.wire.Close()
	}
	for _, sub := range subs {
		sub.stop()
	}
	s.wg.Wait()
	return nil
}

func (s *subscriber) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func (s *subscriber) currentConnection() *subscriberConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

func (s *subscriber) ensureConnectionLocked() (*subscriberConn, error) {
	if conn := s.currentConnection(); conn != nil {
		return conn, nil
	}
	conn, err := s.connectLocked()
	if err != nil {
		return nil, err
	}
	if err := s.restoreSubscriptionsLocked(conn); err != nil {
		s.markDisconnected(conn)
		return nil, err
	}
	return conn, nil
}

func (s *subscriber) connectLocked() (*subscriberConn, error) {
	var lastErr error
	for _, address := range s.cfg.Brokers {
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
		dialCtx, cancel := context.WithTimeout(s.ctx, requestTimeout)
		raw, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", address)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		wire := newWireConn(raw)
		if err := wire.send(protocol.Frame{
			Type:     protocol.TypeHello,
			Version:  protocol.Version,
			ClientID: s.cfg.ClientID,
		}); err != nil {
			_ = wire.Close()
			lastErr = err
			continue
		}
		ctx, cancel := context.WithTimeout(s.ctx, requestTimeout)
		status, err := wire.receive(ctx)
		cancel()
		if err != nil {
			_ = wire.Close()
			lastErr = err
			continue
		}
		if status.Type == protocol.TypeError {
			_ = wire.Close()
			lastErr = responseError(status, "HELLO")
			continue
		}
		if status.Type != protocol.TypeStatus || status.Role != protocol.RolePrincipal || status.Epoch < s.maxEpoch {
			_ = wire.Close()
			lastErr = fmt.Errorf("broker %s não é o principal válido (tipo=%s, papel=%d, época=%d)", address, protocol.TypeName(status.Type), status.Role, status.Epoch)
			continue
		}
		if status.Epoch > s.maxEpoch {
			s.maxEpoch = status.Epoch
			s.mu.Lock()
			s.byTopicID = make(map[uint8]*subscription)
			s.pendingByTopicID = make(map[uint8][]delivery)
			for _, sub := range s.subs {
				sub.topicID = 0
			}
			s.mu.Unlock()
		}

		conn := &subscriberConn{wire: wire, responses: make(chan protocol.Frame, 32)}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = wire.Close()
			return nil, ErrClosed
		}
		s.active = conn
		s.mu.Unlock()

		s.wg.Add(1)
		go s.readLoop(conn)
		return conn, nil
	}
	if lastErr == nil {
		lastErr = errors.New("nenhum broker principal disponível")
	}
	return nil, lastErr
}

func (s *subscriber) resolveTopic(conn *subscriberConn, topic string) (uint8, error) {
	reply, err := s.exchange(conn, protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: topic})
	if err != nil {
		return 0, err
	}
	if reply.Type == protocol.TypeTopicID {
		return reply.TopicID, nil
	}
	if reply.Type != protocol.TypeError || reply.ErrCode != protocol.CodeTopicNotFound {
		return 0, responseError(reply, "RESOLVE_TOPIC")
	}

	reply, err = s.exchange(conn, protocol.Frame{Type: protocol.TypeCreateTopic, TopicName: topic})
	if err != nil {
		return 0, err
	}
	if reply.Type != protocol.TypeTopicID {
		return 0, responseError(reply, "CREATE_TOPIC")
	}
	return reply.TopicID, nil
}

func (s *subscriber) exchange(conn *subscriberConn, request protocol.Frame) (protocol.Frame, error) {
	if s.currentConnection() != conn {
		return protocol.Frame{}, net.ErrClosed
	}
	if err := conn.wire.send(request); err != nil {
		return protocol.Frame{}, err
	}
	ctx, cancel := context.WithTimeout(s.ctx, requestTimeout)
	defer cancel()
	select {
	case reply := <-conn.responses:
		return reply, nil
	case <-conn.wire.done:
		return protocol.Frame{}, conn.wire.connectionError()
	case <-ctx.Done():
		return protocol.Frame{}, ctx.Err()
	}
}

func (s *subscriber) readLoop(conn *subscriberConn) {
	defer s.wg.Done()
	for {
		frame, err := conn.wire.receive(s.ctx)
		if err != nil {
			s.markDisconnected(conn)
			return
		}
		if frame.Type == protocol.TypeMessage {
			s.dispatch(conn.wire, frame)
			continue
		}
		select {
		case conn.responses <- frame:
		case <-conn.wire.done:
			return
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *subscriber) dispatch(conn *wireConn, frame protocol.Frame) {
	item := delivery{
		conn:    conn,
		id:      frame.DeliveryID,
		payload: append([]byte(nil), frame.Payload...),
	}

	s.mu.Lock()
	sub := s.byTopicID[frame.TopicID]
	if sub == nil {
		if !s.closed {
			s.pendingByTopicID[frame.TopicID] = append(s.pendingByTopicID[frame.TopicID], item)
		}
		s.mu.Unlock()
		return // o topic_id ainda será associado quando Subscribe resolver o tópico.
	}
	s.mu.Unlock()

	s.enqueueDelivery(sub, item)
}

// registerSubscriptionID associa um topic_id à assinatura e transfere, em
// ordem, mensagens que chegaram antes de o mapeamento estar disponível.
func (s *subscriber) registerSubscriptionID(sub *subscription, id uint8, startWorker bool) {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()

	s.mu.Lock()
	sub.topicID = id
	s.byTopicID[id] = sub
	pending := s.pendingByTopicID[id]
	delete(s.pendingByTopicID, id)
	s.mu.Unlock()

	if startWorker {
		s.wg.Add(1)
		go s.subscriptionLoop(sub)
	}

	for _, item := range pending {
		select {
		case <-item.conn.done:
			continue
		default:
		}
		select {
		case sub.queue <- item:
		case <-sub.done:
			return
		case <-s.ctx.Done():
			return
		case <-item.conn.done:
			// Sem a conexão original não é possível confirmar; o broker reenviará.
			continue
		}
	}
}

// enqueueDelivery aplica backpressure: quando a fila da assinatura enche,
// a leitora para de consumir o TCP até o handler liberar espaço. Nenhuma
// entrega é descartada silenciosamente por falta de capacidade local.
func (s *subscriber) enqueueDelivery(sub *subscription, item delivery) {
	sub.queueMu.Lock()
	defer sub.queueMu.Unlock()

	select {
	case <-item.conn.done:
		return
	default:
	}
	select {
	case sub.queue <- item:
	case <-sub.done:
	case <-s.ctx.Done():
	case <-item.conn.done:
	}
}

func (s *subscriber) subscriptionLoop(sub *subscription) {
	defer s.wg.Done()
	for {
		select {
		case <-sub.done:
			return
		case <-s.ctx.Done():
			return
		case item := <-sub.queue:
			handler := sub.getHandler()
			if handler == nil || handler(item.payload) != nil {
				continue
			}
			_ = item.conn.send(protocol.Frame{
				Type:       protocol.TypeDeliveryAck,
				DeliveryID: item.id,
			})
		}
	}
}

func (s *subscriber) removeSubscription(sub *subscription) {
	s.mu.Lock()
	if s.subs[sub.topic] == sub {
		delete(s.subs, sub.topic)
	}
	if s.byTopicID[sub.topicID] == sub {
		delete(s.byTopicID, sub.topicID)
	}
	s.mu.Unlock()
}

func (s *subscriber) markDisconnected(conn *subscriberConn) {
	s.mu.Lock()
	if s.active == conn {
		s.active = nil
		// Mantém byTopicID durante uma reconexão na mesma época: o broker
		// pode reenviar pendências imediatamente após HELLO, antes de RESOLVE.
		// Uma época maior limpa o mapa em connectLocked.
	}
	hasSubs := len(s.subs) > 0
	closed := s.closed
	s.mu.Unlock()
	_ = conn.wire.Close()
	if hasSubs && !closed {
		select {
		case s.reconnect <- struct{}{}:
		default:
		}
	}
}

func (s *subscriber) reconnectLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.reconnect:
		}

		delay := initialRetryDelay
		for s.ctx.Err() == nil {
			s.opMu.Lock()
			s.mu.RLock()
			hasSubs := len(s.subs) > 0
			active := s.active
			s.mu.RUnlock()
			var err error
			if hasSubs && active == nil {
				var conn *subscriberConn
				conn, err = s.connectLocked()
				if err == nil {
					err = s.restoreSubscriptionsLocked(conn)
					if err != nil {
						s.markDisconnected(conn)
					}
				}
			}
			s.opMu.Unlock()

			if !hasSubs || err == nil {
				break
			}
			timer := time.NewTimer(delay)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay = nextDelay(delay)
		}
	}
}

func (s *subscriber) restoreSubscriptionsLocked(conn *subscriberConn) error {
	s.mu.RLock()
	subs := make([]*subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.RUnlock()

	for _, sub := range subs {
		id, err := s.resolveTopic(conn, sub.topic)
		if err != nil {
			return err
		}
		s.registerSubscriptionID(sub, id, false)
		reply, err := s.exchange(conn, protocol.Frame{Type: protocol.TypeSubscribe, TopicID: id})
		if err != nil {
			return err
		}
		if reply.Type != protocol.TypeOK {
			return responseError(reply, "SUBSCRIBE")
		}
	}
	return nil
}

func (s *subscriber) handleCommandError(conn *subscriberConn, err error) {
	var brokerErr *brokerError
	if errors.As(err, &brokerErr) && brokerErr.code != protocol.CodeNotLeader && !protocol.IsTransient(brokerErr.code) {
		return
	}
	s.markDisconnected(conn)
}

func responseError(frame protocol.Frame, operation string) error {
	if frame.Type == protocol.TypeError {
		return &brokerError{code: frame.ErrCode}
	}
	return fmt.Errorf("resposta inesperada para %s: %s", operation, protocol.TypeName(frame.Type))
}
