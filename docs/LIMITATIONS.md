# Limitações e próximos passos

O que **não** está no escopo desta entrega (ou é trade-off consciente) e o que faria em produção.

## Trust model

| Camada | Premissa |
|--------|----------|
| Kafka | Authorizer é SoT do snapshot `account.balance`. ACLs no tópico fora deste serviço. |
| API | API key + rate limit opcionais (`make up-secure`). Em prod: JWT/mTLS no gateway, authZ por conta. |
| Dedupe | `(updated_at_micros, last_transaction_id)` — não há tabela `processed_tx`. |

## Trade-offs conscientes (já no código)

1. **Tie-break UUID** em empate de µs é determinístico, não causal. Ideal: sequence monotônico do authorizer.
2. **Retry multi-tópico hop-only:** main → `….retry-1..N` → DLT **sem espera temporal** no consumer (sem `Thread.sleep`). Backoff = hops Kafka, não timer. Se precisar de delay real: `@RetryableTopic` / pause-partition.
3. **Cache on:** put só após Dynamo `saved`; put falho → **DEL**. Ainda pode haver stale ≤ TTL em cenários de race/miss antigo.
4. **CB Dynamo split** `dynamodb-read` / `dynamodb-write` + bulkhead no GET — já implementados (ver DECISIONS §13).
5. **Saldo negativo** no snapshot é aceito (authorizer decide). Amount da **transação** deve ser > 0.
6. **Sem histórico** de txs (só latest). Evolução: SK por timestamp ou Streams.
7. **Rate limit** in-memory (single instance). Multi-pod → gateway/Redis.
8. **Ops lab:** non-root image, graceful shutdown, lag per partition via Micrometer — sem IaC/K8s neste repo.

## Em produção faria (ainda não neste repo)

- Delay temporal entre retry levels se o domínio exigir (hoje hop-only)
- Auth edge obrigatória + actuator em rede privada
- DLT replay tooling + alertas de lag/DLT rate
- IaC, rolling deploy multi-env
- (Opcional) `processed_tx` + TransactWrite se precisarem de identity por tx além da versão composta
