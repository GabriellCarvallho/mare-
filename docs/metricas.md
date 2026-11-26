# Métricas do Maré Pub/Sub

Gerado em 2026-11-26 17:16 · linux/amd64 · 12 CPUs · go1.27.4

Configuração: broker real e clientes no mesmo processo, TCP em 127.0.0.1; log em disco num diretório temporário (/tmp); janela de 32 entregas por assinante; cota de 1000 pendências por sensor. Todas as seções abaixo vêm desta mesma execução.

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
| Encode (binário) | 38 | 2 |
| AppendEncode com buffer reutilizado | 8 | 0 |
| Decode (binário) | 25 | 0 |
| json.Marshal (comparação) | 559 | 3 |
| json.Unmarshal (comparação) | 737 | 0 |

## Vazão e latência — log em memória

5000 leituras de 4 sensores simultâneos (cada sensor espera o PUBLISH_ACK antes da próxima, como a biblioteca), 1 assinante.

| Medida | Valor |
|---|---:|
| Vazão (leituras entregues por segundo) | 30109 |
| Latência PUBLISH → PUBLISH_ACK, p50 (ms) | 0.08 |
| Latência PUBLISH → PUBLISH_ACK, p95 (ms) | 0.39 |
| Latência sensor → assinante, p50 (ms) | 0.74 |
| Latência sensor → assinante, p95 (ms) | 4.90 |
| Entregas repetidas (reenvios) | 0 |

## Vazão e latência — log em disco (wal com fsync)

5000 leituras de 4 sensores simultâneos (cada sensor espera o PUBLISH_ACK antes da próxima, como a biblioteca), 1 assinante.

| Medida | Valor |
|---|---:|
| Vazão (leituras entregues por segundo) | 406 |
| Latência PUBLISH → PUBLISH_ACK, p50 (ms) | 7.32 |
| Latência PUBLISH → PUBLISH_ACK, p95 (ms) | 12.28 |
| Latência sensor → assinante, p50 (ms) | 2215.65 |
| Latência sensor → assinante, p95 (ms) | 7297.25 |
| Entregas repetidas (reenvios) | 0 |

## Distribuição do atendimento com um sensor monopolizador

Consumidor lento (2 ms por leitura). O sensor 1 dispara 1500 leituras normais de uma vez; 50 ms depois, o sensor 2 (críticas) e o sensor 3 (normais) enviam 50 cada, uma por vez.

| Sensor | Aceitas | Recusadas (OVERLOADED) | Latência média até o assinante (ms) | Posição da última entrega |
|---|---:|---:|---:|---:|
| 1 — monopolizador, normais | 1010 | 490 | — | — |
| 2 — críticas | 50 | 0 | 162.25 | 124 de 1110 |
| 3 — normais | 50 | 0 | 328.51 | 207 de 1110 |

## Recuperação depois da promoção — janela RECOVERING de 5s

Sensor publicando 1 leitura a cada 100 ms pela biblioteca cliente; o principal cai e o operador promove a reserva no mesmo instante.

| Medida | Valor |
|---|---:|
| Comando PROMOTE (ms) | 5.08 |
| Queda → primeira leitura nova entregue (s) | 7.0 |
| Leituras publicadas / não entregues | 120 / 0 |

## Recuperação depois da promoção — janela RECOVERING de 30s

Sensor publicando 1 leitura a cada 100 ms pela biblioteca cliente; o principal cai e o operador promove a reserva no mesmo instante.

| Medida | Valor |
|---|---:|
| Comando PROMOTE (ms) | 4.80 |
| Queda → primeira leitura nova entregue (s) | 31.0 |
| Leituras publicadas / não entregues | 360 / 0 |

