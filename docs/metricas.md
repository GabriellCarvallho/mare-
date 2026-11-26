# Métricas do Maré Pub/Sub

Gerado em 2026-11-25 21:03 · linux/amd64 · 12 CPUs · go1.27.4

## Bytes por quadro (prefixo de 2 bytes incluído)

| Quadro | Bytes |
|---|---:|
| PUBLISH de temperatura | 17 |
| MESSAGE de temperatura | 21 |
| PUBLISH_ACK | 3 |
| DELIVERY_ACK | 7 |
| HELLO | 6 |
| STATUS | 8 |
| CREATE_TOPIC "mar/temperatura" | 19 |
| ERROR | 4 |
| Mesma leitura em JSON (sem prefixo) | 135 |

JSON equivalente: `{"type":"PUBLISH","topic":"mar/temperatura","critical":false,"sensor_id":7,"sequence":412,"timestamp":1795556000,"temperature_c":27.71}`

Uma leitura completa (PUBLISH + PUBLISH_ACK + MESSAGE + DELIVERY_ACK) = 48 bytes no binário. O PUBLISH binário é 7.9× menor que o JSON.

## Tempo de codificação e decodificação (PUBLISH de temperatura)

| Operação | ns/op | alocações/op |
|---|---:|---:|
| Encode (binário) | 39 | 2 |
| AppendEncode com buffer reutilizado | 8 | 0 |
| Decode (binário) | 27 | 0 |
| json.Marshal (comparação) | 555 | 3 |
| json.Unmarshal (comparação) | 730 | 0 |

## Vazão e latência — log em memória

5000 leituras de 4 sensores simultâneos (cada sensor espera o PUBLISH_ACK antes da próxima, como a biblioteca), 1 assinante.

| Medida | Valor |
|---|---:|
| Vazão (leituras entregues por segundo) | 25700 |
| Latência PUBLISH → PUBLISH_ACK, p50 (ms) | 0.09 |
| Latência PUBLISH → PUBLISH_ACK, p95 (ms) | 0.51 |
| Latência sensor → assinante, p50 (ms) | 0.94 |
| Latência sensor → assinante, p95 (ms) | 6.42 |

## Vazão e latência — log em disco (wal com fsync)

5000 leituras de 4 sensores simultâneos (cada sensor espera o PUBLISH_ACK antes da próxima, como a biblioteca), 1 assinante.

| Medida | Valor |
|---|---:|
| Vazão (leituras entregues por segundo) | 511 |
| Latência PUBLISH → PUBLISH_ACK, p50 (ms) | 5.87 |
| Latência PUBLISH → PUBLISH_ACK, p95 (ms) | 9.99 |
| Latência sensor → assinante, p50 (ms) | 1518.88 |
| Latência sensor → assinante, p95 (ms) | 5696.44 |

## Distribuição do atendimento com um sensor monopolizador

Consumidor lento (2 ms por leitura). O sensor 1 dispara 1500 leituras normais de uma vez; 50 ms depois, o sensor 2 (críticas) e o sensor 3 (normais) enviam 50 cada, uma por vez.

| Sensor | Aceitas | Recusadas (OVERLOADED) | Latência média até o assinante (ms) | Posição da última entrega |
|---|---:|---:|---:|---:|
| 1 — monopolizador, normais | 1018 | 482 | — | — |
| 2 — críticas | 50 | 0 | 167.64 | 132 de 1118 |
| 3 — normais | 50 | 0 | 341.87 | 215 de 1118 |

## Recuperação depois da promoção — janela RECOVERING de 5s

Sensor publicando 1 leitura a cada 100 ms pela biblioteca cliente; o principal cai e o operador promove a reserva no mesmo instante.

| Medida | Valor |
|---|---:|
| Comando PROMOTE (ms) | 4.72 |
| Queda → primeira leitura nova entregue (s) | 7.0 |
| Leituras publicadas / não entregues | 120 / 0 |

## Recuperação depois da promoção — janela RECOVERING de 30s

Sensor publicando 1 leitura a cada 100 ms pela biblioteca cliente; o principal cai e o operador promove a reserva no mesmo instante.

| Medida | Valor |
|---|---:|
| Comando PROMOTE (ms) | 3.32 |
| Queda → primeira leitura nova entregue (s) | 31.0 |
| Leituras publicadas / não entregues | 360 / 0 |

