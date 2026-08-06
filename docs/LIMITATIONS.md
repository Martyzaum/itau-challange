# Limitações e próximos passos

O que **não** está no escopo desta entrega (ou está em forma lab) e o que faria em produção.

## Trust model

| Camada | Premissa |
|--------|----------|
| Kafka | Authorizer é SoT do snapshot `account.balance`. ACLs no tópico fora deste serviço. |
| API | API key + rate limit opcionais (`make up-secure`). Em prod: JWT/mTLS no gateway, authZ por conta. |
| Dedupe | `(updated_at_micros, last_transaction_id)` — não há tabela `processed_tx`. |

## Conhecidos / trade-offs

1. **Tie-break UUID** em empate de µs é determinístico, não causal. Ideal: sequence monotônico do authorizer.
2. **Retry** é multi-tópico **sem sleep** no consumer (não bloqueia partição). Não há espera real entre níveis além do hop Kafka; se precisar de delay temporal longo: `@RetryableTopic` ou pause-partition.
3. **Cache on:** após Dynamo save, put Redis falho → **invalidate (DEL)**. GET ainda pode ser stale ≤ TTL em miss paths antigos.
4. **CB Dynamo** único GET+write neste branch de hardening progressivo; split read/write é próximo passo.
5. **Saldo negativo** no snapshot é aceito (authorizer decide). Amount de **transação** deve ser > 0.
6. **Sem histórico** de txs (só latest). Evolução: SK por timestamp ou Streams.
7. **Rate limit** in-memory (single instance). Multi-pod → gateway/Redis.
8. **DLT window lab** curta com defaults de delay; prod ajusta `TRANSACTIONS_RETRY_*`.

## Em produção faria

- Non-blocking retry (`@RetryableTopic`)
- CB Dynamo separado read/write + bulkhead no GET
- Auth edge obrigatória + actuator em rede privada
- DLT replay tooling + alertas de lag/DLT rate
- IaC, rolling deploy, graceful shutdown, non-root image
- (Opcional) `processed_tx` + TransactWrite se precisarem de identity por tx além da versão composta
