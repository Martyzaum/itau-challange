package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import io.lettuce.core.api.sync.RedisCommands
import org.slf4j.LoggerFactory
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.UUID

/**
 * Fail-open Redis cache for account balances.
 * Any Redis error is logged and treated as a miss / no-op write — DynamoDB remains source of truth.
 */
class RedisAccountBalanceCache(
    private val commands: RedisCommands<String, String>,
    private val objectMapper: ObjectMapper,
    private val keyPrefix: String,
    private val ttl: Duration?,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun get(accountId: UUID): AccountBalance? =
        try {
            val raw = commands.get(key(accountId)) ?: return null
            objectMapper.readValue(raw, CachedAccountBalancePayload::class.java).toDomain()
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_get_failed accountId={} error={}", accountId, ex.toString())
            null
        }

    /**
     * Stores [balance] only if absent or strictly newer than the cached version (composite version).
     * Best-effort under concurrency; DynamoDB conditional write remains authoritative.
     */
    fun putIfNewer(balance: AccountBalance) {
        try {
            val existing = get(balance.id)
            if (existing != null && !balance.isNewerThan(existing)) {
                return
            }
            val payload = objectMapper.writeValueAsString(CachedAccountBalancePayload.from(balance))
            val redisKey = key(balance.id)
            if (ttl != null && !ttl.isZero && !ttl.isNegative) {
                commands.setex(redisKey, ttl.seconds.coerceAtLeast(1), payload)
            } else {
                commands.set(redisKey, payload)
            }
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_put_failed accountId={} error={}", balance.id, ex.toString())
        }
    }

    private fun key(accountId: UUID): String = "$keyPrefix$accountId"
}
