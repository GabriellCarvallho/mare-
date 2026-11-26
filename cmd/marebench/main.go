// marebench mede as métricas da Seção 9.3 do documento de projeto com o
// broker real rodando no mesmo processo (server.Run) e clientes TCP.
//
//	go run ./cmd/marebench                 # tudo, saída em Markdown
//	go run ./cmd/marebench -n 20000        # mais mensagens na vazão
//	go run ./cmd/marebench -skip-recovery  # sem o teste de promoção (~40 s)
//
// Mede: bytes por quadro (e o mesmo dado em JSON), tempo de codificação e
// decodificação, vazão e latência p50/p95 com o log em memória e em disco
// (fsync), distribuição do atendimento com um sensor monopolizador, e o
// tempo de recuperação depois da promoção da reserva.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/sched"
	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/walstore"
	"github.com/GabriellCarvallho/mare-pubsub/client"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

var (
	nMsgs        = flag.Int("n", 5000, "mensagens no teste de vazão")
	publishers   = flag.Int("publishers", 4, "sensores simultâneos no teste de vazão")
	skipRecovery = flag.Bool("skip-recovery", false, "pula o teste de promoção")
)

func main() {
	flag.Parse()
	fmt.Printf("# Métricas do Maré Pub/Sub\n\nGerado em %s · %s/%s · %d CPUs · %s\n\n",
		time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Printf("Configuração: broker real e clientes no mesmo processo, TCP em 127.0.0.1; log em disco num diretório temporário (%s); "+
		"janela de %d entregas por assinante; cota de %d pendências por sensor. Todas as seções abaixo vêm desta mesma execução.\n\n",
		os.TempDir(), server.DefaultSendWindow, sched.DefaultPerPublisher)

	bytesPorQuadro()
	codificacao()
	for _, disco := range []bool{false, true} {
		vazao(disco)
	}
	distribuicao()
	if !*skipRecovery {
		for _, janela := range []time.Duration{5 * time.Second, server.DefaultRecovery} {
			recuperacao(janela)
		}
	}
}

// ---------------------------------------------------------------------------
// Payload e JSON de comparação

func leitura(sensor uint16, seq uint32) []byte {
	p := make([]byte, 12)
	binary.BigEndian.PutUint16(p[0:], sensor)
	binary.BigEndian.PutUint32(p[2:], seq)
	binary.BigEndian.PutUint32(p[6:], 1795556000)
	binary.BigEndian.PutUint16(p[10:], 2771) // 27,71 °C
	return p
}

// leituraJSON é o mesmo PUBLISH num formato textual típico, com nomes de
// campo repetidos em cada mensagem (a alternativa da Seção 8).
type leituraJSON struct {
	Type      string  `json:"type"`
	Topic     string  `json:"topic"`
	Critical  bool    `json:"critical"`
	SensorID  uint16  `json:"sensor_id"`
	Sequence  uint32  `json:"sequence"`
	Timestamp uint32  `json:"timestamp"`
	TempC     float64 `json:"temperature_c"`
}

var exemploJSON = leituraJSON{"PUBLISH", "mar/temperatura", false, 7, 412, 1795556000, 27.71}

// ---------------------------------------------------------------------------
// 1. Bytes por quadro

func tamanho(f protocol.Frame) int {
	b, err := protocol.Encode(f)
	if err != nil {
		panic(err)
	}
	return len(b)
}

func bytesPorQuadro() {
	p := leitura(7, 412)
	js, _ := json.Marshal(exemploJSON)
	fmt.Println("## Bytes por quadro (prefixo de 2 bytes incluído)")
	fmt.Println()
	fmt.Println("| Quadro | Bytes |")
	fmt.Println("|---|---:|")
	linhas := []struct {
		nome string
		f    protocol.Frame
	}{
		{"PUBLISH de temperatura", protocol.Frame{Type: protocol.TypePublish, TopicID: 1, Payload: p}},
		{"MESSAGE de temperatura", protocol.Frame{Type: protocol.TypeMessage, TopicID: 1, DeliveryID: 99, Payload: p}},
		{"PUBLISH_ACK", protocol.Frame{Type: protocol.TypePublishAck}},
		{"DELIVERY_ACK", protocol.Frame{Type: protocol.TypeDeliveryAck, DeliveryID: 99}},
		{"HELLO", protocol.Frame{Type: protocol.TypeHello, Version: 1, ClientID: 7}},
		{"STATUS", protocol.Frame{Type: protocol.TypeStatus, Role: protocol.RolePrincipal, Epoch: 1}},
		{"CREATE_TOPIC \"mar/temperatura\"", protocol.Frame{Type: protocol.TypeCreateTopic, TopicName: "mar/temperatura"}},
		{"ERROR", protocol.Frame{Type: protocol.TypeError, ErrCode: protocol.CodeRecovering}},
	}
	for _, l := range linhas {
		fmt.Printf("| %s | %d |\n", l.nome, tamanho(l.f))
	}
	pub := tamanho(linhas[0].f)
	fmt.Printf("| Mesma leitura em JSON (sem prefixo) | %d |\n\n", len(js))
	fmt.Printf("JSON equivalente: `%s`\n\n", js)
	fmt.Printf("Uma leitura completa (PUBLISH + PUBLISH_ACK + MESSAGE + DELIVERY_ACK) = %d bytes no binário. O PUBLISH binário é %.1f× menor que o JSON.\n\n",
		pub+tamanho(linhas[2].f)+tamanho(linhas[1].f)+tamanho(linhas[3].f), float64(len(js))/float64(pub))
}

// ---------------------------------------------------------------------------
// 2. Codificação e decodificação

func codificacao() {
	f := protocol.Frame{Type: protocol.TypePublish, TopicID: 1, Payload: leitura(7, 412)}
	enc, _ := protocol.Encode(f)
	body := enc[2:]
	js, _ := json.Marshal(exemploJSON)

	medir := func(fn func(b *testing.B)) testing.BenchmarkResult { return testing.Benchmark(fn) }
	res := []struct {
		nome string
		r    testing.BenchmarkResult
	}{
		{"Encode (binário)", medir(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				protocol.Encode(f)
			}
		})},
		{"AppendEncode com buffer reutilizado", medir(func(b *testing.B) {
			b.ReportAllocs()
			buf := make([]byte, 0, 64)
			for i := 0; i < b.N; i++ {
				buf, _ = protocol.AppendEncode(buf[:0], f)
			}
		})},
		{"Decode (binário)", medir(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				protocol.Decode(body)
			}
		})},
		{"json.Marshal (comparação)", medir(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				json.Marshal(exemploJSON)
			}
		})},
		{"json.Unmarshal (comparação)", medir(func(b *testing.B) {
			b.ReportAllocs()
			var v leituraJSON
			for i := 0; i < b.N; i++ {
				json.Unmarshal(js, &v)
			}
		})},
	}
	fmt.Println("## Tempo de codificação e decodificação (PUBLISH de temperatura)")
	fmt.Println()
	fmt.Println("| Operação | ns/op | alocações/op |")
	fmt.Println("|---|---:|---:|")
	for _, x := range res {
		fmt.Printf("| %s | %d | %d |\n", x.nome, x.r.NsPerOp(), x.r.AllocsPerOp())
	}
	fmt.Println()
}

// ---------------------------------------------------------------------------
// Broker no mesmo processo

func subir(disco bool, role string, addr string, dataDir string, recovery time.Duration) (string, func()) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	if dataDir == "" {
		d, err := os.MkdirTemp("", "marebench-")
		if err != nil {
			panic(err)
		}
		dataDir = d
	}
	cfg := server.Config{
		Addr: addr, Role: role, DataDir: dataDir, Recovery: recovery,
		Logf: func(string, ...any) {},
	}
	if disco {
		cfg.OpenLog = walstore.Open
	} else {
		cfg.OpenLog = server.NewMemoryStore(0).Open
	}
	got := make(chan string, 1)
	cfg.OnListen = func(a net.Addr) { got <- a.String() }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, cfg) }()
	select {
	case a := <-got:
		return a, func() { cancel(); <-done }
	case err := <-done:
		panic(err)
	}
}

// conexão crua do protocolo (sem a biblioteca), para medir o broker.
type raw struct{ c net.Conn }

func conectar(addr string, id uint16) *raw {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	r := &raw{c}
	r.send(protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version, ClientID: id})
	if f := r.recv(); f.Type != protocol.TypeStatus {
		panic(fmt.Sprint("HELLO: ", f))
	}
	return r
}

func (r *raw) send(f protocol.Frame) {
	if err := protocol.WriteFrame(r.c, f); err != nil {
		panic(err)
	}
}

func (r *raw) recv() protocol.Frame {
	f, err := protocol.ReadFrame(r.c)
	if err != nil {
		panic(err)
	}
	return f
}

func (r *raw) topico(nome string) uint8 {
	r.send(protocol.Frame{Type: protocol.TypeCreateTopic, TopicName: nome})
	f := r.recv()
	if f.Type != protocol.TypeTopicID {
		panic(fmt.Sprint("CREATE_TOPIC: ", f))
	}
	return f.TopicID
}

// falhar encerra o medidor com erro: um resultado incompleto não é publicado
// como se fosse válido.
func falhar(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "marebench: "+format+"\n", args...)
	os.Exit(1)
}

// chave monta a chave (sensor_id, sequence) de uma leitura.
func chave(sensor uint16, seq uint32) uint64 { return uint64(sensor)<<32 | uint64(seq) }

// assinanteCru: confirma cada MESSAGE e anota a hora da primeira chegada por
// chave (sensor_id, sequence). total conta todos os MESSAGE, inclusive
// repetições. atraso simula um consumidor lento.
type assinanteCru struct {
	mu      sync.Mutex
	chegada map[uint64]time.Time
	ordem   []uint64
	total   atomic.Int64
}

// unicas devolve quantas leituras distintas já chegaram.
func (a *assinanteCru) unicas() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.chegada)
}

// esperar aguarda n leituras distintas; se o limite passar, o medidor falha.
func (a *assinanteCru) esperar(n int, limite time.Duration, etapa string) {
	fim := time.Now().Add(limite)
	for a.unicas() < n {
		if time.Now().After(fim) {
			falhar("%s: só %d de %d leituras distintas chegaram em %s", etapa, a.unicas(), n, limite)
		}
		time.Sleep(time.Millisecond)
	}
}

func assinar(addr string, id uint16, topico string, atraso time.Duration) *assinanteCru {
	r := conectar(addr, id)
	tid := r.topico(topico)
	r.send(protocol.Frame{Type: protocol.TypeSubscribe, TopicID: tid})
	if f := r.recv(); f.Type != protocol.TypeOK {
		panic(fmt.Sprint("SUBSCRIBE: ", f))
	}
	a := &assinanteCru{chegada: make(map[uint64]time.Time)}
	go func() {
		for {
			f, err := protocol.ReadFrame(r.c)
			if err != nil {
				return
			}
			if f.Type != protocol.TypeMessage {
				continue
			}
			k := chave(binary.BigEndian.Uint16(f.Payload), binary.BigEndian.Uint32(f.Payload[2:]))
			agora := time.Now()
			a.mu.Lock()
			if _, ok := a.chegada[k]; !ok {
				a.chegada[k] = agora
				a.ordem = append(a.ordem, k)
			}
			a.mu.Unlock()
			a.total.Add(1)
			if atraso > 0 {
				time.Sleep(atraso)
			}
			protocol.WriteFrame(r.c, protocol.Frame{Type: protocol.TypeDeliveryAck, DeliveryID: f.DeliveryID})
		}
	}()
	return a
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[int(p*float64(len(d)-1))]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d.Microseconds())/1000) }

// ---------------------------------------------------------------------------
// 3. Vazão e latência

func vazao(disco bool) {
	nome := "memória"
	if disco {
		nome = "disco (wal com fsync)"
	}
	addr, stop := subir(disco, server.RolePrincipal, "", "", 0)
	defer stop()
	sub := assinar(addr, 100, "mar/temperatura", 0)

	n := *nMsgs
	porSensor := n / *publishers
	var mu sync.Mutex
	envio := make(map[uint64]time.Time, n)
	var ackLat []time.Duration
	var wg sync.WaitGroup
	inicio := time.Now()
	for p := 0; p < *publishers; p++ {
		wg.Add(1)
		go func(sensor uint16) {
			defer wg.Done()
			r := conectar(addr, sensor)
			r.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/temperatura"})
			tid := r.recv().TopicID
			local := make([]time.Duration, 0, porSensor)
			for s := 1; s <= porSensor; s++ {
				t0 := time.Now()
				mu.Lock()
				envio[chave(sensor, uint32(s))] = t0
				mu.Unlock()
				r.send(protocol.Frame{Type: protocol.TypePublish, TopicID: tid, Flags: byte(s % 2), Payload: leitura(sensor, uint32(s))})
				if f := r.recv(); f.Type != protocol.TypePublishAck {
					panic(fmt.Sprint("PUBLISH: ", f))
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			ackLat = append(ackLat, local...)
			mu.Unlock()
		}(uint16(1 + p))
	}
	wg.Wait()
	total := porSensor * *publishers
	sub.esperar(total, 5*time.Minute, "vazão ("+nome+")")
	dur := time.Since(inicio)

	var e2e []time.Duration
	sub.mu.Lock()
	for k, t0 := range envio {
		t, ok := sub.chegada[k]
		if !ok {
			falhar("vazão (%s): leitura %d/%d não chegou", nome, k>>32, uint32(k))
		}
		e2e = append(e2e, t.Sub(t0))
	}
	repetidas := sub.total.Load() - int64(len(sub.chegada))
	sub.mu.Unlock()

	fmt.Printf("## Vazão e latência — log em %s\n\n", nome)
	fmt.Printf("%d leituras de %d sensores simultâneos (cada sensor espera o PUBLISH_ACK antes da próxima, como a biblioteca), 1 assinante.\n\n", total, *publishers)
	fmt.Println("| Medida | Valor |")
	fmt.Println("|---|---:|")
	fmt.Printf("| Vazão (leituras entregues por segundo) | %.0f |\n", float64(total)/dur.Seconds())
	fmt.Printf("| Latência PUBLISH → PUBLISH_ACK, p50 (ms) | %s |\n", ms(pct(ackLat, 0.50)))
	fmt.Printf("| Latência PUBLISH → PUBLISH_ACK, p95 (ms) | %s |\n", ms(pct(ackLat, 0.95)))
	fmt.Printf("| Latência sensor → assinante, p50 (ms) | %s |\n", ms(pct(e2e, 0.50)))
	fmt.Printf("| Latência sensor → assinante, p95 (ms) | %s |\n", ms(pct(e2e, 0.95)))
	fmt.Printf("| Entregas repetidas (reenvios) | %d |\n\n", repetidas)
}

// ---------------------------------------------------------------------------
// 4. Distribuição do atendimento: um sensor monopolizador

func distribuicao() {
	addr, stop := subir(false, server.RolePrincipal, "", "", 0)
	defer stop()
	sub := assinar(addr, 100, "mar/temperatura", 2*time.Millisecond) // consumidor lento: forma fila

	// Sensor 1: dispara 1.500 leituras normais sem esperar ACK.
	var aceitas, sobrecarga atomic.Int64
	r1 := conectar(addr, 1)
	r1.send(protocol.Frame{Type: protocol.TypeResolveTopic, TopicName: "mar/temperatura"})
	tid := r1.recv().TopicID
	const flood = 1500
	done1 := make(chan struct{})
	go func() {
		for i := 0; i < flood; i++ {
			f := r1.recv()
			if f.Type == protocol.TypePublishAck {
				aceitas.Add(1)
			} else if f.ErrCode == protocol.CodeOverloaded {
				sobrecarga.Add(1)
			}
		}
		close(done1)
	}()
	for s := 1; s <= flood; s++ {
		r1.send(protocol.Frame{Type: protocol.TypePublish, TopicID: tid, Payload: leitura(1, uint32(s))})
	}
	time.Sleep(50 * time.Millisecond)

	// Sensores 2 (críticas) e 3 (normais): 50 leituras cada, uma por vez.
	type res struct {
		aceitas, recusadas int
		envio              map[uint32]time.Time // só as aceitas
	}
	resultado := make([]res, 4)
	var wg sync.WaitGroup
	for _, sensor := range []uint16{2, 3} {
		wg.Add(1)
		go func(sensor uint16) {
			defer wg.Done()
			r := conectar(addr, sensor)
			env := make(map[uint32]time.Time)
			ok, recusadas := 0, 0
			for s := 1; s <= 50; s++ {
				var flags byte
				if sensor == 2 {
					flags = protocol.FlagCritical
				}
				t0 := time.Now()
				r.send(protocol.Frame{Type: protocol.TypePublish, TopicID: tid, Flags: flags, Payload: leitura(sensor, uint32(s))})
				if r.recv().Type == protocol.TypePublishAck {
					ok++
					env[uint32(s)] = t0
				} else {
					recusadas++
				}
			}
			resultado[sensor] = res{ok, recusadas, env}
		}(sensor)
	}
	wg.Wait()
	<-done1

	esperado := int(aceitas.Load()) + resultado[2].aceitas + resultado[3].aceitas
	sub.esperar(esperado, 2*time.Minute, "distribuição")
	sub.mu.Lock()
	pos := make(map[uint64]int)
	for i, k := range sub.ordem {
		pos[k] = i + 1
	}
	media := func(sensor uint16) (time.Duration, int) {
		env := resultado[sensor].envio
		if len(env) == 0 {
			falhar("distribuição: nenhuma leitura do sensor %d foi aceita", sensor)
		}
		var soma time.Duration
		ultima := 0
		for s, t := range env {
			k := chave(sensor, s)
			c, ok := sub.chegada[k]
			if !ok {
				falhar("distribuição: leitura %d/%d foi aceita, mas não chegou ao assinante", sensor, s)
			}
			soma += c.Sub(t)
			if pos[k] > ultima {
				ultima = pos[k]
			}
		}
		return soma / time.Duration(len(env)), ultima
	}
	m2, u2 := media(2)
	m3, u3 := media(3)
	sub.mu.Unlock()

	fmt.Println("## Distribuição do atendimento com um sensor monopolizador")
	fmt.Println()
	fmt.Printf("Consumidor lento (2 ms por leitura). O sensor 1 dispara %d leituras normais de uma vez; 50 ms depois, o sensor 2 (críticas) e o sensor 3 (normais) enviam 50 cada, uma por vez.\n\n", flood)
	fmt.Println("| Sensor | Aceitas | Recusadas (OVERLOADED) | Latência média até o assinante (ms) | Posição da última entrega |")
	fmt.Println("|---|---:|---:|---:|---:|")
	fmt.Printf("| 1 — monopolizador, normais | %d | %d | — | — |\n", aceitas.Load(), sobrecarga.Load())
	fmt.Printf("| 2 — críticas | %d | %d | %s | %d de %d |\n", resultado[2].aceitas, resultado[2].recusadas, ms(m2), u2, len(pos))
	fmt.Printf("| 3 — normais | %d | %d | %s | %d de %d |\n\n", resultado[3].aceitas, resultado[3].recusadas, ms(m3), u3, len(pos))
}

// ---------------------------------------------------------------------------
// 5. Tempo de recuperação depois da promoção (biblioteca cliente)

func recuperacao(janela time.Duration) {
	addrP, stopP := subir(true, server.RolePrincipal, "", "", 0)
	addrR, stopR := subir(true, server.RoleReserva, "", "", janela)
	defer stopR()
	brokers := []string{addrP, addrR}

	var mu sync.Mutex
	chegada := make(map[uint32]time.Time)
	sub, err := client.NewSubscriber(client.Config{Brokers: brokers, ClientID: 100})
	if err != nil {
		panic(err)
	}
	defer sub.Close()
	sub.Subscribe("mar/temperatura", func(p []byte) error {
		mu.Lock()
		if _, ok := chegada[binary.BigEndian.Uint32(p[2:])]; !ok {
			chegada[binary.BigEndian.Uint32(p[2:])] = time.Now()
		}
		mu.Unlock()
		return nil
	})
	qdir, _ := os.MkdirTemp("", "marebench-q-")
	pub, err := client.NewPublisher(client.Config{Brokers: brokers, ClientID: 7, QueueDir: qdir})
	if err != nil {
		panic(err)
	}
	defer pub.Close()

	// Uma leitura a cada 100 ms; o principal cai depois de 2 s.
	stopPub := make(chan struct{})
	envio := make(map[uint32]time.Time)
	var seq uint32
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopPub:
				return
			case <-t.C:
				mu.Lock()
				seq++
				s := seq
				envio[s] = time.Now()
				mu.Unlock()
				pub.Publish("mar/temperatura", false, leitura(7, s))
			}
		}
	}()
	time.Sleep(2 * time.Second)

	queda := time.Now()
	stopP()
	t0 := time.Now()
	if _, err := server.Promote(addrR); err != nil {
		panic(err)
	}
	promover := time.Since(t0)

	// Espera a primeira leitura feita depois da queda chegar, e todas as
	// anteriores também.
	var primeira time.Time
	limite := time.Now().Add(janela + 70*time.Second)
	for time.Now().Before(limite) {
		mu.Lock()
		for s, t := range envio {
			if t.After(queda) {
				if c, ok := chegada[s]; ok && (primeira.IsZero() || c.Before(primeira)) {
					primeira = c
				}
			}
		}
		mu.Unlock()
		if !primeira.IsZero() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	close(stopPub)
	time.Sleep(2 * time.Second)

	mu.Lock()
	faltando := 0
	for s := range envio {
		if _, ok := chegada[s]; !ok && envio[s].Before(time.Now().Add(-2*time.Second)) {
			faltando++
		}
	}
	total := len(envio)
	mu.Unlock()

	if primeira.IsZero() {
		falhar("recuperação (%s): nenhuma leitura feita depois da queda chegou", janela)
	}
	if faltando > 0 {
		falhar("recuperação (%s): %d de %d leituras não foram entregues", janela, faltando, total)
	}

	fmt.Printf("## Recuperação depois da promoção — janela RECOVERING de %s\n\n", janela)
	fmt.Println("Sensor publicando 1 leitura a cada 100 ms pela biblioteca cliente; o principal cai e o operador promove a reserva no mesmo instante.")
	fmt.Println()
	fmt.Println("| Medida | Valor |")
	fmt.Println("|---|---:|")
	fmt.Printf("| Comando PROMOTE (ms) | %s |\n", ms(promover))
	fmt.Printf("| Queda → primeira leitura nova entregue (s) | %.1f |\n", primeira.Sub(queda).Seconds())
	fmt.Printf("| Leituras publicadas / não entregues | %d / %d |\n\n", total, faltando)
}
