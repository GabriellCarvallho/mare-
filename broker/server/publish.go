package server

// Passo 5: publicação e entrega (Seções 4.1, 4.2, 4.3 e 7).
//
// Caminho de uma leitura:
//
//	PUBLISH ─► snapshot dos assinantes ─► ref + escalonador ─► PUBLISH_ACK
//	                                            │
//	              despachante (goroutine) ◄─────┘ Ready()
//	                    │ delivery.Add: uma entrega por assinante
//	                    ▼
//	              MESSAGE ─► assinante ─► DELIVERY_ACK ─► delivery.Ack
//	                    ▲                                     │
//	   relógio (goroutine): Tick a cada 250 ms                └► Pending == 0:
//	   reenvia o que venceu o prazo; 10ª falha = quarentena       sched.Done, apaga
//	                                                              a publicação, Purge
//
// Todo o estado (pubs, sched, dm, clients) fica sob b.mu.

import (
	"context"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/delivery"
	"github.com/GabriellCarvallho/mare-pubsub/broker/sched"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// publication é uma leitura aceita pelo broker e ainda não concluída.
type publication struct {
	ref        uint64
	topic      uint8
	flags      byte
	publisher  uint16
	payload    []byte
	recipients []uint16   // snapshot dos assinantes no momento do PUBLISH
	item       sched.Item // o item no escalonador (para o Done)

	quotaReleased bool // sched.Done já foi chamado
	quarantined   int  // entregas que foram para a quarentena
}

// ---------------------------------------------------------------------------
// PUBLISH (5.2)

// publish: snapshot → ref → escalonador → PUBLISH_ACK.
// No passo 6, o PUBLISH_ACK só sai depois de gravar no log.
func (b *broker) publish(s *session, f protocol.Frame) protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Foto dos assinantes agora: quem assinar depois não recebe esta leitura.
	// Sem assinantes, a leitura é descartada e o sensor é avisado (4.2).
	recipients, err := b.topics.Snapshot(f.TopicID)
	if err != nil {
		return topicError(err) // NO_SUBSCRIBERS, TOPIC_NOT_FOUND, TOPIC_REMOVING
	}

	ref := b.nextRef
	item := sched.Item{Publisher: s.clientID, Critical: f.Critical(), Ref: ref}
	if err := b.sched.Enqueue(item); err != nil {
		return errFrame(protocol.CodeOverloaded) // sensor passou da cota (7)
	}
	b.nextRef++

	b.pubs[ref] = &publication{
		ref:        ref,
		topic:      f.TopicID,
		flags:      f.Flags,
		publisher:  s.clientID,
		payload:    append([]byte(nil), f.Payload...),
		recipients: recipients,
		item:       item,
	}
	return protocol.Frame{Type: protocol.TypePublishAck}
}

// ---------------------------------------------------------------------------
// Despachante (5.3)

// dispatchLoop tira publicações do escalonador (na ordem 3 críticas : 1
// normal, com rodízio por sensor) e cria as entregas.
func (b *broker) dispatchLoop(ctx context.Context) {
	defer b.wg.Done()
	for {
		b.mu.Lock()
		for {
			it, ok := b.sched.Next()
			if !ok {
				break
			}
			if p := b.pubs[it.Ref]; p != nil {
				b.startDeliveries(p)
			}
		}
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-b.sched.Ready(): // chegou publicação nova
		}
	}
}

// startDeliveries cria uma entrega por destinatário e tenta enviar.
// Chamar com b.mu travado.
func (b *broker) startDeliveries(p *publication) {
	now := b.now()
	for _, d := range b.dm.Add(p.ref, p.recipients) {
		b.send(d, now)
	}
}

// target escolhe a conexão do assinante: a mais recente que fez SUBSCRIBE;
// se nenhuma fez (o cliente reconectou), a mais recente. Chamar com b.mu.
func (b *broker) target(client uint16) *session {
	list := b.clients[client]
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].subscriber {
			return list[i]
		}
	}
	if len(list) > 0 {
		return list[len(list)-1]
	}
	return nil
}

// send tenta entregar um MESSAGE sem bloquear. Assinante desconectado ou
// com a fila cheia: a entrega continua AGUARDANDO, sem contar falha, e o
// relógio tenta de novo. Chamar com b.mu travado.
func (b *broker) send(d delivery.Delivery, now time.Time) {
	s := b.target(d.Client)
	p := b.pubs[d.Ref]
	if s == nil || p == nil {
		return
	}
	msg := protocol.Frame{
		Type:       protocol.TypeMessage,
		Flags:      p.flags,
		TopicID:    p.topic,
		DeliveryID: d.ID,
		Payload:    p.payload,
	}
	if s.trySend(msg) {
		b.dm.MarkSent(d.ID, now) // começa a contar o prazo do ACK
	}
}

// trySend enfileira um quadro sem bloquear; false se a fila estiver cheia.
// Um assinante lento não pode travar o despachante (e com ele os outros).
func (s *session) trySend(f protocol.Frame) bool {
	select {
	case s.out <- f:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// DELIVERY_ACK (5.4)

// ack confirma uma entrega. Não tem resposta. ACK repetido, atrasado ou de
// outro cliente é ignorado.
func (b *broker) ack(s *session, id uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ref, ok := b.dm.Ack(id, s.clientID); ok {
		b.afterChange(ref)
	}
}

// afterChange atualiza a publicação depois de um ACK ou de uma quarentena.
// Chamar com b.mu travado.
func (b *broker) afterChange(ref uint64) {
	p := b.pubs[ref]
	if p == nil || b.dm.Pending(ref) > 0 {
		return // ainda faltam entregas
	}
	if !p.quotaReleased {
		b.sched.Done(p.item) // libera a cota do sensor
		p.quotaReleased = true
	}
	if p.quarantined > 0 {
		return // quarentena não é conclusão (4.3): a publicação fica
	}
	delete(b.pubs, ref) // concluída
	b.maybePurge(p.topic)
}

// ---------------------------------------------------------------------------
// Relógio de reenvio (5.5) e reconexão (5.6)

func (b *broker) retryLoop(ctx context.Context) {
	defer b.wg.Done()
	t := time.NewTicker(b.cfg.RetryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.tick()
		}
	}
}

// kick roda um tick já, sem esperar o próximo (usado quando um cliente
// volta e pode ter entregas pendentes).
func (b *broker) kick() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.tick()
	}()
}

// tick vence prazos, reenvia e põe em quarentena (10ª falha seguida).
func (b *broker) tick() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.role != protocol.RolePrincipal {
		return
	}

	now := b.now()
	online := func(c uint16) bool { return len(b.clients[c]) > 0 }
	send, quarantined := b.dm.Tick(now, online)

	for _, d := range send {
		b.send(d, now)
	}
	for _, d := range quarantined {
		b.cfg.Logf("entrega %d (ref %d, cliente %d) em quarentena após %d falhas",
			d.ID, d.Ref, d.Client, d.Failures)
		if p := b.pubs[d.Ref]; p != nil {
			p.quarantined++
		}
		b.afterChange(d.Ref)
	}
}
