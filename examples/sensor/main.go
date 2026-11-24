package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/client"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	brokersFlag := flag.String("brokers", "127.0.0.1:9000", "endereços dos brokers separados por vírgula")
	sensorFlag := flag.Uint("sensor-id", 7, "identificador do sensor (1 a 65535)")
	topicFlag := flag.String("topic", "mar/temperatura", "tópico das leituras")
	thresholdFlag := flag.Float64("critical-above", 29.0, "temperatura acima da qual a leitura é crítica, em °C")
	queueFlag := flag.String("queue-dir", "", "diretório da fila persistente (padrão: data/sensor-ID)")
	intervalFlag := flag.Duration("interval", time.Second, "intervalo entre leituras")
	flag.Parse()

	if *sensorFlag == 0 || *sensorFlag > math.MaxUint16 {
		return errors.New("sensor-id deve estar entre 1 e 65535")
	}
	if *intervalFlag <= 0 {
		return errors.New("interval deve ser maior que zero")
	}
	brokers := splitBrokers(*brokersFlag)
	if len(brokers) == 0 {
		return errors.New("informe ao menos um broker")
	}

	sensorID := uint16(*sensorFlag)
	queueDir := *queueFlag
	if queueDir == "" {
		queueDir = filepath.Join("data", "sensor-"+strconv.Itoa(int(sensorID)))
	}
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		return fmt.Errorf("criar diretório do sensor: %w", err)
	}
	sequencePath := filepath.Join(queueDir, "sequence.state")
	sequence, err := loadSequence(sequencePath)
	if err != nil {
		return fmt.Errorf("recuperar contador do sensor: %w", err)
	}

	publisher, err := client.NewPublisher(client.Config{
		Brokers:  brokers,
		ClientID: sensorID,
		QueueDir: queueDir,
		OnError: func(topic string, _ []byte, publishErr error) {
			log.Printf("publicação descartada no tópico %q: %v", topic, publishErr)
		},
	})
	if err != nil {
		return fmt.Errorf("criar publicador: %w", err)
	}
	defer publisher.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// A variação senoidal simula uma temperatura que oscila ao redor de 25 °C.
	// Uma pequena parcela aleatória evita leituras sempre idênticas.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(*intervalFlag)
	defer ticker.Stop()

	log.Printf("sensor %d publicando em %q; brokers: %s", sensorID, *topicFlag, strings.Join(brokers, ", "))
	for {
		now := time.Now()
		nextSequence := sequence + 1
		if nextSequence == 0 {
			return errors.New("contador de leituras uint32 esgotado")
		}
		// Reserva o número em disco antes de enfileirar a leitura. Se houver
		// uma queda depois daqui, pode existir um salto, mas nunca reutilização.
		if err := saveSequence(sequencePath, nextSequence); err != nil {
			return fmt.Errorf("persistir contador do sensor: %w", err)
		}
		sequence = nextSequence

		temperature := 25 + 6*math.Sin(float64(nextSequence)*0.17) + (rng.Float64()-0.5)*0.3
		payload, err := encodeReading(sensorID, nextSequence, uint32(now.Unix()), temperature)
		if err != nil {
			return err
		}
		critical := temperature > *thresholdFlag
		if err := publisher.Publish(*topicFlag, critical, payload); err != nil {
			if errors.Is(err, client.ErrQueueFull) {
				log.Printf("fila local cheia; leitura %d não foi armazenada (contador reservado)", nextSequence)
			} else if errors.Is(err, client.ErrClosed) {
				return nil
			} else {
				return fmt.Errorf("publicar leitura %d: %w", nextSequence, err)
			}
		} else {
			priority := "NORMAL"
			if critical {
				priority = "CRÍTICA"
			}
			log.Printf("leitura=%d temperatura=%.2f °C prioridade=%s", sequence, temperature, priority)
		}

		select {
		case <-ctx.Done():
			log.Println("encerrando sensor; leituras ainda pendentes permanecem na fila local")
			return nil
		case <-ticker.C:
		}
	}
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

// encodeReading monta o payload de 12 bytes definido no documento do projeto.
func encodeReading(sensorID uint16, sequence, timestamp uint32, temperatureC float64) ([]byte, error) {
	centiDegrees := math.Round(temperatureC * 100)
	if centiDegrees < math.MinInt16 || centiDegrees > math.MaxInt16 {
		return nil, fmt.Errorf("temperatura %.2f °C fora do intervalo representável", temperatureC)
	}

	payload := make([]byte, 12)
	binary.BigEndian.PutUint16(payload[0:2], sensorID)
	binary.BigEndian.PutUint32(payload[2:6], sequence)
	binary.BigEndian.PutUint32(payload[6:10], timestamp)
	binary.BigEndian.PutUint16(payload[10:12], uint16(int16(centiDegrees)))
	return payload, nil
}
