# Maré Pub/Sub

Middleware Publish/Subscribe para sensores marítimos — IFPB, Programação Distribuída.

## Parte 1 (Gabriel): protocolo binário e `maredump`

```bash
go test -race ./...                         # testes
go test ./protocol -run x -fuzz FuzzDecode  # fuzzing
go run ./cmd/maredump -payload temperatura 000F01000100070000019C6AE1F1D00A96
```

| Arquivo | Conteúdo |
|---|---|
| `protocol/protocol.go` | `Frame`, constantes de tipo, flags, papéis e códigos de erro |
| `protocol/codec.go` | `Encode`, `AppendEncode`, `Decode`, `ReadFrame`, `WriteFrame` |
| `protocol/testdata/vectors.json` | vetores compartilhados (válidos, inválidos, truncados) |
| `cmd/maredump/` | ferramenta de diagnóstico: hex → texto |

### Decisões a confirmar com o Felipe

O documento fixa só PUBLISH = `0x01` e MESSAGE = `0x02`. Demais códigos:

| Tipo | Código | Corpo |
|---|---|---|
| PUBLISH_ACK | `0x03` | só o tipo |
| DELIVERY_ACK | `0x04` | delivery_id (4) |
| HELLO | `0x10` | versão (1) + client_id (2) |
| STATUS | `0x11` | papel (1: principal, 2: reserva) + época (4) |
| CREATE_TOPIC / RESOLVE_TOPIC | `0x20` / `0x21` | len (1) + nome UTF-8 (1–63) |
| TOPIC_ID (resposta) | `0x22` | topic_id (1) |
| DELETE_TOPIC / SUBSCRIBE / UNSUBSCRIBE | `0x23` / `0x24` / `0x25` | topic_id (1) |
| OK (resposta) | `0x26` | só o tipo |
| ERROR | `0x30` | código (1) |

Códigos de erro: `0x01` INVALID_FRAME, `0x02` INVALID_MESSAGE, `0x03` UNSUPPORTED_VERSION,
`0x04` HELLO_REQUIRED, `0x10` NO_SUBSCRIBERS, `0x11` TOPIC_NOT_FOUND, `0x12` TOPIC_REMOVING,
`0x13` TOO_MANY_TOPICS, `0x20` OVERLOADED, `0x21` STORAGE_UNAVAILABLE, `0x22` RECOVERING,
`0x30` NOT_LEADER. NOT_LEADER e RECOVERING são códigos dentro de ERROR, não comandos próprios.

Outras decisões:
- **Payload máximo de 249 bytes.** O MESSAGE tem 4 bytes a mais de cabeçalho; um PUBLISH
  com 250–253 bytes caberia em 256, mas não poderia ser entregue.
- **Erros:** `Encode` devolve `ErrFieldRange` para valor fora do campo e `ErrInvalidFrame`
  para tipo desconhecido ou corpo > 256. Do lado de quem recebe, tudo é `ErrInvalidFrame`.
- **`Decode` não copia o payload:** `Frame.Payload` aponta para o buffer lido.
  `ReadFrame` aloca um buffer novo por quadro, então é seguro guardá-lo.
- Extras além do contrato: `AppendEncode` (sem alocação), `Decode`, `Frame.String`,
  `TypeName`, `CodeName`, `IsTerminal`, `IsTransient`.

## Parte 2 (Gabriel): tópicos, assinaturas e escalonador

Tudo em memória, sem rede e sem disco.

| Arquivo | Conteúdo |
|---|---|
| `broker/topics/topics.go` | `Registry`: topic_id 1–255, `Create` idempotente, EM_REMOÇÃO, `Purge`, assinaturas, `Snapshot` |
| `broker/sched/sched.go` | `Scheduler`: 3 críticas para 1 normal, rodízio por publicador, cota de 1.000, limite global |

### Comportamentos definidos onde o contrato era omisso

**topics**
- `Create` usa o **menor topic_id livre**. É determinístico: repetir a mesma sequência de
  operações gera os mesmos códigos.
- Em EM_REMOÇÃO o topic_id **continua ocupado** até o `Purge`; `Create` do mesmo nome
  devolve `ErrTopicRemoving`.
- `Resolve` de tópico em remoção devolve o id (o cliente recebe TOPIC_REMOVING ao publicar).
- `Delete` é idempotente.
- `Snapshot` devolve os client_id em ordem crescente.
- Extensões (avisar o Felipe): `ErrNotRemoving` (Purge de tópico ativo), `ErrIDInUse` e o
  método `Restore(id, name)` para a Parte 3 recriar tópicos com o id gravado no log.

**sched**
- Construtor `New(Config{})`; valores zero usam o padrão (1.000 por publicador,
  100.000 na fila global, rajada de 3 críticos).
- A cota por publicador conta itens **não concluídos**: sair da fila com `Next` não libera;
  só `Done` libera. A fila global conta só os itens **aguardando**.
- Sem itens normais, críticos saem sem pausa; normal que chega depois de uma rajada sai
  em no máximo 3 críticos.
- `Done` é idempotente (indexado por `Ref`). Extensão: `ErrDuplicateRef` para o mesmo `Ref`
  enfileirado duas vezes antes do `Done`.

Uso esperado pelo broker (Parte 3):

```go
for {
    it, ok := s.Next()
    if !ok {
        <-s.Ready() // espera um Enqueue
        continue
    }
    // ... cria as entregas; quando todas terminarem: s.Done(it)
}
```
