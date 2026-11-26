# Maré Pub/Sub

Middleware Publish/Subscribe em Go para sensores marítimos de temperatura, com protocolo binário próprio sobre TCP, entrega pelo menos uma vez, prioridade para leituras críticas, log em disco e broker reserva com promoção manual.

Projeto da disciplina Programação Distribuída (IFPB, Prof. Ruan Delgado Gomes), feito por Gabriel Pereira de Carvalho e Felipe Raimundo.

## Rodando em 1 minuto

Requer Go 1.24 ou mais novo. Use um terminal por linha:

```bash
go run ./cmd/broker -role principal -addr :7000 -data ./dados-p
go run ./cmd/broker -role reserva   -addr :7001 -data ./dados-r
go run ./examples/consumer -brokers localhost:7000,localhost:7001 -client-id 100
go run ./examples/sensor   -brokers localhost:7000,localhost:7001 -sensor-id 7
```

O sensor publica uma leitura por segundo, e o consumidor imprime cada uma. Para simular a queda do principal, aperte Ctrl+C no primeiro terminal e promova a reserva:

```bash
go run ./cmd/broker -promote localhost:7001
```

Os clientes trocam de broker sozinhos. Depois da janela RECOVERING (30 s), as leituras guardadas na fila local do sensor chegam ao consumidor sem perda.

## Testes e métricas

```bash
gofmt -l . && go vet ./... && go test -race ./...   # tudo, como no CI
go test -race -v ./contracttest/cenarios             # cenários da Seção 9.2, de ponta a ponta
go run ./cmd/marebench                               # métricas da Seção 9.3 (~1 min)
go run ./cmd/maredump -payload temperatura 000F01000100070000019C6AE1F1D00A96
```

Resultados medidos estão em [`docs/metricas.md`](docs/metricas.md). Em resumo, com o log em disco (fsync a cada confirmação):

| Medida | Valor |
|---|---:|
| PUBLISH de temperatura | 17 bytes (a mesma leitura em JSON: 135 bytes) |
| Encode / Decode do PUBLISH | ~50 ns / ~40 ns |
| Vazão, 4 sensores, 1 assinante | ~2.500 a 2.800 leituras/s |
| Latência PUBLISH → PUBLISH_ACK, p50 / p95 | ~1,2 ms / ~3,3 ms |
| Latência sensor → assinante, p50 / p95 | ~1,9 ms / ~4,1 ms |
| Leitura crítica com consumidor lento e um sensor inundando | ~165 ms (sem a janela: ~2,4 s) |
| Queda do principal → primeira leitura nova entregue | janela RECOVERING + 1 a 2 s (31 s com a janela padrão de 30 s) |

## Arquitetura

```
 sensor ──► client.Publisher ──TCP──► broker principal ──TCP──► client.Subscriber ──► consumidor
            (fila local em disco)      │  log em disco (wal)      (deduplica por sensor+sequence)
                                       │
                                       └── reserva vazia, promovida manualmente (PROMOTE)
```

| Pacote | O que faz | Autor |
|---|---|---|
| `protocol` | quadro binário, `Encode`/`Decode`, `ReadFrame`/`WriteFrame`, vetores de teste | Gabriel |
| `cmd/maredump` | converte quadros de hex para texto | Gabriel |
| `broker/topics` | tópicos (id 1 a 255), EM_REMOÇÃO, assinaturas duráveis | Gabriel |
| `broker/sched` | fila com 3 críticas para 1 normal, rodízio por sensor, cota de 1.000 | Gabriel |
| `broker/logrec` | os 9 tipos de registro do log | Gabriel |
| `broker/delivery` | estado de cada entrega: prazo, recuo, quarentena | Gabriel |
| `broker/server` | o broker: conexões, comandos, entregas, janela por assinante, log, reinício, reserva e promoção | Gabriel |
| `broker/walstore` | liga o `wal` ao broker | Gabriel |
| `cmd/broker` | executável do broker | Gabriel |
| `client/localqueue` | fila local persistente do sensor (10 MiB) | Felipe |
| `broker/wal` | log em disco com CRC32, seq, fsync e compactação | Felipe |
| `client` | biblioteca: `Publisher`, `Subscriber`, reconexão, época, reenvio | Felipe |
| `examples/sensor`, `examples/consumer` | aplicações de exemplo | Felipe |
| `contracttest/cenarios` | testes de ponta a ponta da Seção 9.2 | Gabriel |
| `cmd/marebench` | medidor das métricas da Seção 9.3 | Gabriel |

**Prioridade de verdade:** cada assinante tem uma janela de 32 entregas em voo (`Config.SendWindow`). As outras aguardam no broker, e cada vaga que abre (ACK, prazo vencido, reconexão) é preenchida pela regra do escalonador: até 3 críticas para cada normal e rodízio entre os sensores. Sem a janela, tudo ia de uma vez para a conexão do assinante, que é FIFO, e uma leitura crítica esperava atrás das normais.

**Concorrência no broker:** cada conexão tem uma goroutine leitora (processa os pedidos em ordem, então as respostas saem na ordem dos pedidos) e uma escritora (a única que escreve no socket). Todo o estado compartilhado fica sob um mutex. Duas goroutines de fundo: o despachante (escalonador → entregas) e o relógio de reenvio.

**Confirmação:** o broker grava no log e faz fsync **antes** de responder PUBLISH_ACK. Ao reiniciar, relê o log e recompõe tópicos, assinaturas e entregas pendentes.

## Protocolo

Quadro = tamanho do corpo (uint16 big-endian) + corpo de 1 a 256 bytes. O corpo começa pelo tipo (1 byte).

| Comando | Código | Corpo depois do tipo | Bytes no fio |
|---|---|---|---:|
| PUBLISH | `0x01` | flags (bit 0 = crítica), topic_id, payload | 5 + payload |
| MESSAGE | `0x02` | flags, topic_id, delivery_id (4), payload | 9 + payload |
| PUBLISH_ACK | `0x03` | — (casa com o PUBLISH pela ordem) | 3 |
| DELIVERY_ACK | `0x04` | delivery_id (4); sem resposta | 7 |
| HELLO | `0x10` | versão (1), client_id (2) | 6 |
| STATUS | `0x11` | papel (1 principal, 2 reserva), época (4) | 8 |
| PROMOTE | `0x12` | — (operador → reserva) | 3 |
| CREATE_TOPIC / RESOLVE_TOPIC | `0x20` / `0x21` | tamanho (1), nome UTF-8 (1 a 63) | 4 + nome |
| TOPIC_ID | `0x22` | topic_id | 4 |
| DELETE_TOPIC / SUBSCRIBE / UNSUBSCRIBE | `0x23` / `0x24` / `0x25` | topic_id | 4 |
| OK | `0x26` | — | 3 |
| ERROR | `0x30` | código | 4 |

| Erros | Códigos | O cliente faz |
|---|---|---|
| quadro rejeitado | `0x01` INVALID_FRAME, `0x02` INVALID_MESSAGE, `0x03` UNSUPPORTED_VERSION, `0x04` HELLO_REQUIRED | corrige; não insiste |
| terminais | `0x10` NO_SUBSCRIBERS, `0x11` TOPIC_NOT_FOUND, `0x12` TOPIC_REMOVING, `0x13` TOO_MANY_TOPICS | avisa a aplicação (`OnError`) e descarta |
| transitórios | `0x20` OVERLOADED, `0x21` STORAGE_UNAVAILABLE, `0x22` RECOVERING | mantém na fila local e tenta com recuo |
| liderança | `0x30` NOT_LEADER, `0x31` ALREADY_PRINCIPAL | procura o principal válido |

**Payload de temperatura** (12 bytes, big-endian, definido pela aplicação): `sensor_id` uint16 · `sequence` uint32 · `timestamp` uint32 (Unix) · `temp` int16 em centésimos de °C. Exemplo: `00 07 00 00 01 9C 6A E1 F1 D0 0A 96` = sensor 7, leitura 412, 27,10 °C.

## Parâmetros

| Parâmetro | Valor | Onde |
|---|---|---|
| Corpo máximo do quadro / payload máximo | 256 / 249 bytes | `protocol` |
| Prazo do DELIVERY_ACK / recuo máximo | 5 s / 60 s | `broker/delivery` |
| Falhas antes da quarentena | 10 | `broker/delivery` |
| Escalonamento | 3 críticas : 1 normal, rodízio por sensor | `broker/sched` |
| Pendências por sensor | 1.000 (excedente: OVERLOADED) | `broker/sched` |
| Entregas em voo por assinante | 32 | `broker/server` |
| Log do broker | 512 MiB (excedente: STORAGE_UNAVAILABLE) | `broker/walstore` |
| Fila local do sensor | 10 MiB (excedente: `client.ErrQueueFull`) | `client` |
| Janela RECOVERING | 30 s (`-recovery`) | `cmd/broker` |

## Decisões de implementação

- **Payload máximo de 249 bytes**, e não 253: o MESSAGE tem 4 bytes a mais de cabeçalho, e todo PUBLISH aceito precisa poder ser entregue.
- **Época = segundos Unix da promoção**, gravada em `DATA/epoch` de forma atômica. A reserva não conhece a época do principal antigo; o relógio garante um número maior. Se o relógio estiver atrasado, usa a anterior + 1. O cliente recusa broker com época menor.
- **Um arquivo de log por época** (`DATA/log-<época>.wal`). A reserva promovida começa num log novo e vazio, como pede a Seção 6.2.
- **topic_id = o menor livre**: determinístico, o que facilita os testes. Em EM_REMOÇÃO, o id fica ocupado até a última entrega.
- **Reconexão sem perder a vez:** quando um assinante volta, o broker manda primeiro o STATUS e depois as pendências; a biblioteca guarda as mensagens que chegam antes de o tópico ser resolvido.
- **Deduplicação na aplicação** pelo par `sensor_id` + `sequence` (conjunto das sequências já vistas), como pede a Seção 4.3.

## Limitações conhecidas

- **Consumidor mais lento que os sensores:** a janela por assinante garante a ordem (críticas primeiro, rodízio), mas a espera se acumula no broker até a cota de 1.000 por sensor. Com o log em memória, onde os sensores publicam muito mais rápido, isso aparece como latência de dezenas a centenas de ms no p95.
- **O publicador manda uma leitura por vez** (espera o PUBLISH_ACK antes da próxima), o que limita a vazão de um sensor sozinho ao tempo de um fsync.
- **Sem autenticação nem criptografia** (Seção 3.6): rede de testes apenas.
- **Promoção manual:** o operador precisa isolar o principal antigo antes de promover a reserva.