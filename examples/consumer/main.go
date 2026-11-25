package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/client"
)

type reading struct {
	SensorID     uint16
	Sequence     uint32
	Timestamp    uint32
	TemperatureC float64
}

// readingDeduplicator identifica cada leitura pelo par (sensor_id, sequence).
// Não usa apenas a maior sequência, pois o escalonador pode entregar mensagens
// CRÍTICAS antes de mensagens NORMAL mais antigas.
type readingDeduplicator struct {
	mu   sync.Mutex
	seen map[uint16]map[uint32]struct{}
}

// Seen retorna true quando a sequência deste sensor já foi processada.
func (d *readingDeduplicator) Seen(sensorID uint16, sequence uint32) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	sequences := d.seen[sensorID]
	if sequences == nil {
		sequences = make(map[uint32]struct{})
		d.seen[sensorID] = sequences
	}
	if _, exists := sequences[sequence]; exists {
		return true
	}
	sequences[sequence] = struct{}{}
	return false
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	brokersFlag := flag.String("brokers", "127.0.0.1:7000", "endereços dos brokers separados por vírgula")
	clientIDFlag := flag.Uint("client-id", 100, "identificador deste consumidor (1 a 65535)")
	topicFlag := flag.String("topic", "mar/temperatura", "tópico assinado")
	flag.Parse()

	if *clientIDFlag == 0 || *clientIDFlag > 65535 {
		return errors.New("client-id deve estar entre 1 e 65535")
	}
	brokers := splitBrokers(*brokersFlag)
	if len(brokers) == 0 {
		return errors.New("informe ao menos um broker")
	}

	subscriber, err := client.NewSubscriber(client.Config{
		Brokers:  brokers,
		ClientID: uint16(*clientIDFlag),
	})
	if err != nil {
		return fmt.Errorf("criar assinante: %w", err)
	}
	defer subscriber.Close()

	deduper := &readingDeduplicator{seen: make(map[uint16]map[uint32]struct{})}
	handler := func(payload []byte) error {
		item, err := decodeReading(payload)
		if err != nil {
			// Payload inválido é irrecuperável para esta aplicação: registra o
			// problema e confirma para não repetir até a quarentena.
			log.Printf("mensagem descartada: payload inválido: %v", err)
			return nil
		}

		if deduper.Seen(item.SensorID, item.Sequence) {
			return nil // duplicata exata: confirma sem processar novamente
		}

		measuredAt := time.Unix(int64(item.Timestamp), 0).UTC()
		log.Printf("sensor=%d leitura=%d horário=%s temperatura=%.2f °C",
			item.SensorID, item.Sequence, measuredAt.Format(time.RFC3339), item.TemperatureC)
		return nil
	}

	if err := subscriber.Subscribe(*topicFlag, handler); err != nil {
		return fmt.Errorf("assinar tópico %q: %w", *topicFlag, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	log.Printf("consumidor %d inscrito em %q; brokers: %s", *clientIDFlag, *topicFlag, strings.Join(brokers, ", "))
	<-ctx.Done()
	log.Println("encerrando consumidor")
	return nil
}

func splitBrokers(value string) []string {
	var brokers []string
	for _, address := range strings.Split(value, ",") {
		if address = strings.TrimSpace(address); address != "" {
			brokers = append(brokers, address)
		}
	}
	return brokers
}

// decodeReading interpreta o payload big-endian de 12 bytes do cenário marítimo.
func decodeReading(payload []byte) (reading, error) {
	if len(payload) != 12 {
		return reading{}, fmt.Errorf("payload de temperatura inválido: %d bytes (esperados 12)", len(payload))
	}
	temp := int16(binary.BigEndian.Uint16(payload[10:12]))
	return reading{
		SensorID:     binary.BigEndian.Uint16(payload[0:2]),
		Sequence:     binary.BigEndian.Uint32(payload[2:6]),
		Timestamp:    binary.BigEndian.Uint32(payload[6:10]),
		TemperatureC: float64(temp) / 100,
	}, nil
}
