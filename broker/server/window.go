package server

// Janela de envio por assinante (Seção 7: prioridade e justiça).
//
// O despachante cria as entregas assim que o escalonador libera as
// publicações. Se elas fossem todas enviadas de uma vez, a fila real passaria
// a ser a da conexão do assinante, que é FIFO: com um consumidor lento, uma
// leitura crítica esperaria atrás de centenas de normais já enviadas.
//
// Por isso cada assinante tem uma janela: no máximo Config.SendWindow
// entregas em voo (enviadas e sem DELIVERY_ACK). As outras aguardam no
// broker. Quando abre uma vaga (ACK, prazo vencido, reconexão), fill escolhe
// a próxima pela mesma regra do escalonador:
//
//   - até 3 críticas para cada normal, quando há das duas classes aguardando;
//   - dentro de cada classe, rodízio entre os sensores (publicadores).
//
// Assim a ordem de atendimento é decidida no broker, onde as leituras
// ainda podem ser reordenadas, e não na fila da conexão.

import (
	"sort"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/delivery"
	"github.com/GabriellCarvallho/mare-pubsub/broker/sched"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// DefaultSendWindow é o padrão de Config.SendWindow.
const DefaultSendWindow = 32

// fairness guarda, por assinante, o estado da regra 3:1 e do rodízio
// entre uma chamada de fill e a próxima.
type fairness struct {
	streak  int       // críticas seguidas enviadas enquanto havia normais aguardando
	lastPub [2]uint16 // último sensor atendido em cada classe (0 normal, 1 crítica)
	hasLast [2]bool
}

const (
	classNormal   = 0
	classCritical = 1
)

// fill envia entregas aguardando do assinante até encher a janela.
// Chamar com b.mu travado.
func (b *broker) fill(client uint16, now time.Time) {
	free := b.cfg.SendWindow - b.dm.InFlight(client)
	if free <= 0 || b.target(client) == nil {
		return
	}
	waiting := b.dm.Waiting(client)
	if len(waiting) == 0 {
		return
	}

	// Filas por classe e, dentro da classe, por sensor (em ordem de chegada).
	var queues [2]map[uint16][]delivery.Delivery
	queues[classNormal] = make(map[uint16][]delivery.Delivery)
	queues[classCritical] = make(map[uint16][]delivery.Delivery)
	var counts [2]int
	for _, d := range waiting {
		p := b.pubs[d.Ref]
		if p == nil {
			continue
		}
		c := classNormal
		if p.flags&protocol.FlagCritical != 0 {
			c = classCritical
		}
		queues[c][p.publisher] = append(queues[c][p.publisher], d)
		counts[c]++
	}

	f := b.fair[client]
	if f == nil {
		f = &fairness{}
		b.fair[client] = f
	}
	for free > 0 && counts[classNormal]+counts[classCritical] > 0 {
		// Escolhe a classe sem mexer no estado: ele só muda depois que send
		// confirmar o envio, para uma tentativa que falhou não gastar uma das
		// vagas da rajada de críticas.
		c := classNormal
		streak := 0
		switch {
		case counts[classNormal] == 0:
			c = classCritical // só críticas: nenhuma normal esperando por elas
		case counts[classCritical] > 0 && f.streak < criticalBurst(b.cfg.Sched):
			c = classCritical
			streak = f.streak + 1
		}

		pub := nextPublisher(queues[c], f.lastPub[c], f.hasLast[c])
		d := queues[c][pub][0]
		if !b.send(d, now) {
			return // fila da conexão cheia: o relógio tenta de novo
		}
		f.streak = streak
		f.lastPub[c], f.hasLast[c] = pub, true
		queues[c][pub] = queues[c][pub][1:]
		if len(queues[c][pub]) == 0 {
			delete(queues[c], pub)
		}
		counts[c]--
		free--
	}
}

// nextPublisher escolhe, no rodízio, o primeiro sensor depois do último
// atendido (em ordem de client_id, voltando ao início).
func nextPublisher(q map[uint16][]delivery.Delivery, last uint16, hasLast bool) uint16 {
	pubs := make([]uint16, 0, len(q))
	for p := range q {
		pubs = append(pubs, p)
	}
	sort.Slice(pubs, func(i, j int) bool { return pubs[i] < pubs[j] })
	if hasLast {
		for _, p := range pubs {
			if p > last {
				return p
			}
		}
	}
	return pubs[0]
}

// criticalBurst devolve a rajada de críticas do escalonador (padrão 3).
func criticalBurst(cfg sched.Config) int {
	if cfg.CriticalBurst > 0 {
		return cfg.CriticalBurst
	}
	return sched.DefaultCriticalBurst
}
