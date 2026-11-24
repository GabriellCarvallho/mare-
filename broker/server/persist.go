package server

// Passo 6: gravar no log e reconstruir o estado ao reiniciar (Seção 5.1).
//
// Regra de ouro: primeiro grava, depois muda a memória e responde. Assim,
// tudo que o cliente viu confirmado está no log; se o broker cair logo
// depois, ao voltar ele relê o log e chega ao mesmo estado.
//
// O que é gravado (tipos do pacote logrec):
//
//	CREATE_TOPIC novo   → TOPIC_CREATED      SUBSCRIBE   → SUBSCRIBED
//	DELETE_TOPIC        → TOPIC_REMOVING     UNSUBSCRIBE → UNSUBSCRIBED
//	tópico sem pendência → TOPIC_PURGED      PUBLISH     → PUBLISHED (antes do PUBLISH_ACK)
//	DELIVERY_ACK        → DELIVERED          10ª falha   → QUARANTINED
//	início do principal → EPOCH

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/GabriellCarvallho/mare-pubsub/broker/logrec"
)

// logPath: um arquivo de log por época de liderança. No passo 7, a reserva
// promovida ganha uma época nova e, com ela, um log novo e vazio.
func logPath(dir string, epoch uint32) string {
	return filepath.Join(dir, fmt.Sprintf("log-%010d.wal", epoch))
}

// appendRec codifica e grava um registro. Chamar com b.mu travado.
// ErrLogBroken encerra o broker (Run devolve o erro): sem disco confiável,
// ele não pode continuar confirmando publicações.
func (b *broker) appendRec(r logrec.Record) error {
	data, err := logrec.Encode(r)
	if err != nil {
		return err
	}
	if b.log == nil {
		return ErrLogBroken
	}
	_, err = b.log.Append(Record{Kind: r.Kind, Data: data})
	if errors.Is(err, ErrLogBroken) {
		b.fail(err)
	}
	return err
}

// fail avisa o Run de um erro fatal (só o primeiro importa).
func (b *broker) fail(err error) {
	select {
	case b.fatal <- err:
	default:
	}
}

// openLog abre o log da época atual e reconstrói o estado.
func (b *broker) openLog() error {
	l, err := b.cfg.OpenLog(logPath(b.cfg.DataDir, b.epoch))
	if err != nil {
		return err
	}
	b.log = l
	if err := b.replay(); err != nil {
		l.Close()
		b.log = nil
		return fmt.Errorf("reconstrução do log: %w", err)
	}
	return nil
}

// replay relê o log do início e reconstrói tópicos, assinaturas e as
// publicações ainda não concluídas. Chamado antes de aceitar conexões.
func (b *broker) replay() error {
	// Estado de cada publicação lida: quem já confirmou e quem foi para a
	// quarentena.
	type pubState struct {
		p           *publication
		delivered   map[uint16]bool
		quarantined map[uint16]bool
	}
	found := map[uint64]*pubState{}
	var order []uint64 // ordem em que as publicações aparecem no log
	records := 0

	err := b.log.Replay(func(seq uint64, raw Record) error {
		r, err := logrec.Decode(raw.Kind, raw.Data)
		if err != nil {
			return fmt.Errorf("registro %d: %w", seq, err)
		}
		records++

		switch r.Kind {
		case logrec.KindTopicCreated:
			// Restore (Parte 2) recria o tópico com o MESMO id gravado.
			if err := b.topics.Restore(r.TopicID, r.TopicName); err != nil {
				return fmt.Errorf("registro %d: %w", seq, err)
			}
		case logrec.KindTopicRemoving:
			b.topics.Delete(r.TopicID)
			b.removing[r.TopicID] = true
		case logrec.KindTopicPurged:
			b.topics.Purge(r.TopicID)
			delete(b.removing, r.TopicID)
		case logrec.KindSubscribed:
			b.topics.Subscribe(r.TopicID, r.Client)
		case logrec.KindUnsubscribed:
			b.topics.Unsubscribe(r.TopicID, r.Client)

		case logrec.KindPublished:
			found[r.Ref] = &pubState{
				p: &publication{
					ref:        r.Ref,
					topic:      r.TopicID,
					flags:      r.Flags,
					publisher:  r.Publisher,
					payload:    r.Payload,
					recipients: r.Recipients,
					// Recuperadas não passam pelo escalonador: a cota já foi
					// "paga" antes da queda e não precisa ser liberada.
					quotaReleased: true,
				},
				delivered:   map[uint16]bool{},
				quarantined: map[uint16]bool{},
			}
			order = append(order, r.Ref)
			if r.Ref >= b.nextRef {
				b.nextRef = r.Ref + 1
			}
		case logrec.KindDelivered:
			if ps := found[r.Ref]; ps != nil {
				ps.delivered[r.Client] = true
			}
		case logrec.KindQuarantined:
			if ps := found[r.Ref]; ps != nil {
				ps.quarantined[r.Client] = true
			}
		case logrec.KindEpoch:
			// A época de verdade vem no passo 7.
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Recria as entregas que faltavam. Os delivery_id são novos: um ACK
	// antigo será ignorado e a mensagem reenviada ("pelo menos uma vez").
	for _, ref := range order {
		ps := found[ref]
		var left []uint16
		for _, c := range ps.p.recipients {
			if !ps.delivered[c] && !ps.quarantined[c] {
				left = append(left, c)
			}
		}
		if len(left) == 0 && len(ps.quarantined) == 0 {
			continue // concluída antes da queda
		}
		ps.p.quarantined = len(ps.quarantined)
		b.pubs[ref] = ps.p
		b.dm.Add(ref, left)
	}

	// Tópicos que estavam em remoção e ficaram sem pendências.
	for id := range b.removing {
		b.maybePurge(id)
	}

	b.logRecords = records
	b.cfg.Logf("log relido: %d registros, %d publicações pendentes", records, len(b.pubs))
	return nil
}

// maybeCompact remove do log as publicações já concluídas, para ele não
// crescer para sempre (Seção 5.1). Roda a cada CompactEvery conclusões.
// Chamar com b.mu travado.
func (b *broker) maybeCompact() {
	b.completed++
	if b.completed < b.cfg.CompactEvery || b.log == nil {
		return
	}
	err := b.log.Compact(func(_ uint64, raw Record) bool {
		switch raw.Kind {
		case logrec.KindPublished, logrec.KindDelivered, logrec.KindQuarantined:
			r, err := logrec.Decode(raw.Kind, raw.Data)
			if err != nil {
				return true // na dúvida, mantém
			}
			_, live := b.pubs[r.Ref]
			return live // só fica o que ainda não terminou
		}
		return true // tópicos, assinaturas e época são pequenos: ficam
	})
	if err != nil {
		b.cfg.Logf("compactação falhou: %v", err)
		return
	}
	b.completed = 0
}
