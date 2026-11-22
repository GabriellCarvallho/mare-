package server

// Passo 4: tópicos e assinaturas pela rede (Seções 3.5 e 5.2).
//
// Aqui o servidor só traduz: recebe o comando, chama o Registry da Parte 2 e
// converte o erro do Go no código de erro do protocolo.
//
// Passo 6: cada mudança é gravada no log ANTES de responder. Se a gravação
// falhar, a mudança é desfeita e o cliente recebe STORAGE_UNAVAILABLE.

import (
	"errors"

	"github.com/GabriellCarvallho/mare-pubsub/broker/logrec"
	"github.com/GabriellCarvallho/mare-pubsub/broker/topics"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// topicError converte os erros do pacote topics em ERROR do protocolo.
func topicError(err error) protocol.Frame {
	switch {
	case errors.Is(err, topics.ErrTopicNotFound):
		return errFrame(protocol.CodeTopicNotFound)
	case errors.Is(err, topics.ErrTopicRemoving):
		return errFrame(protocol.CodeTopicRemoving)
	case errors.Is(err, topics.ErrTooManyTopics):
		return errFrame(protocol.CodeTooManyTopics)
	case errors.Is(err, topics.ErrNoSubscribers):
		return errFrame(protocol.CodeNoSubscribers)
	}
	return errFrame(protocol.CodeInvalidMessage)
}

var storageUnavailable = errFrame(protocol.CodeStorageUnavailable)

func topicID(id uint8) protocol.Frame {
	return protocol.Frame{Type: protocol.TypeTopicID, TopicID: id}
}

var ok = protocol.Frame{Type: protocol.TypeOK}

// CREATE_TOPIC → TOPIC_ID. Idempotente: o mesmo nome devolve o mesmo id, o
// que permite aos clientes recriar os tópicos na reserva promovida.
func (b *broker) createTopic(name string) protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Nome que já existe: devolve o mesmo id, sem gravar nada de novo.
	if id, err := b.topics.Resolve(name); err == nil && !b.removing[id] {
		return topicID(id)
	}

	id, err := b.topics.Create(name)
	if err != nil {
		return topicError(err)
	}
	if err := b.appendRec(logrec.Record{Kind: logrec.KindTopicCreated, TopicID: id, TopicName: name}); err != nil {
		b.topics.Delete(id) // desfaz: o tópico não pode existir só na memória
		b.topics.Purge(id)
		return storageUnavailable
	}
	return topicID(id)
}

// RESOLVE_TOPIC → TOPIC_ID do nome já existente.
func (b *broker) resolveTopic(name string) protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	id, err := b.topics.Resolve(name)
	if err != nil {
		return topicError(err)
	}
	return topicID(id)
}

// DELETE_TOPIC → OK. O tópico vai para EM_REMOÇÃO: recusa novas publicações
// e assinaturas. Só é removido de vez (Purge, liberando o topic_id) quando
// não houver mais entregas pendentes que o usem.
func (b *broker) deleteTopic(id uint8) protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.removing[id] {
		return ok // já está em remoção (idempotente)
	}
	// Confere se o tópico existe antes de gravar (Snapshot devolve
	// ErrTopicNotFound para id inexistente).
	if _, err := b.topics.Snapshot(id); errors.Is(err, topics.ErrTopicNotFound) {
		return topicError(err)
	}
	if err := b.appendRec(logrec.Record{Kind: logrec.KindTopicRemoving, TopicID: id}); err != nil {
		return storageUnavailable
	}
	b.topics.Delete(id)
	b.removing[id] = true
	b.maybePurge(id)
	return ok
}

// maybePurge remove de vez um tópico EM_REMOÇÃO sem pendências.
// Chamar com b.mu travado.
func (b *broker) maybePurge(id uint8) {
	if !b.removing[id] || b.hasPending(id) {
		return
	}
	if err := b.appendRec(logrec.Record{Kind: logrec.KindTopicPurged, TopicID: id}); err != nil {
		return // tenta de novo na próxima conclusão
	}
	if err := b.topics.Purge(id); err == nil {
		delete(b.removing, id)
	}
}

// hasPending diz se alguma publicação deste tópico ainda está no broker
// (com entregas pendentes ou em quarentena). Chamar com b.mu travado.
func (b *broker) hasPending(id uint8) bool {
	for _, p := range b.pubs {
		if p.topic == id {
			return true
		}
	}
	return false
}

// SUBSCRIBE → OK. A assinatura fica ligada ao client_id do HELLO e continua
// valendo se o cliente desconectar (Seção 4.2).
func (b *broker) subscribe(s *session, id uint8) protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.topics.Subscribe(id, s.clientID); err != nil {
		return topicError(err)
	}
	if err := b.appendRec(logrec.Record{Kind: logrec.KindSubscribed, TopicID: id, Client: s.clientID}); err != nil {
		b.topics.Unsubscribe(id, s.clientID) // desfaz
		return storageUnavailable
	}
	s.subscriber = true // esta conexão passa a ser a preferida para os MESSAGE
	return ok
}

// UNSUBSCRIBE → OK. Impede novas entregas; as já criadas continuam.
func (b *broker) unsubscribe(s *session, id uint8) protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.topics.Unsubscribe(id, s.clientID); err != nil {
		return topicError(err)
	}
	if err := b.appendRec(logrec.Record{Kind: logrec.KindUnsubscribed, TopicID: id, Client: s.clientID}); err != nil {
		b.topics.Subscribe(id, s.clientID) // desfaz
		return storageUnavailable
	}
	return ok
}
