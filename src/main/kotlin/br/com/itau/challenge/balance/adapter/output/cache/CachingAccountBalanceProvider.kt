package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbAccountBalanceProvider
import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * Cache-aside read: Redis → on miss DynamoDB → fill cache.
 * Redis failures fall through to DynamoDB (fail-open).
 */
@Component
@Primary
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class CachingAccountBalanceProvider(
    private val dynamoDbAccountBalanceProvider: DynamoDbAccountBalanceProvider,
    private val redisAccountBalanceCache: RedisAccountBalanceCache,
    private val balanceMetrics: BalanceMetrics,
) : AccountBalanceProvider {
    override fun findByAccountId(accountId: UUID): AccountBalance? {
        val cached = redisAccountBalanceCache.get(accountId)
        if (cached != null) {
            balanceMetrics.incrementCacheHit()
            return cached
        }
        balanceMetrics.incrementCacheMiss()
        val loaded = dynamoDbAccountBalanceProvider.findByAccountId(accountId) ?: return null
        redisAccountBalanceCache.putIfNewer(loaded)
        return loaded
    }
}
