package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbAccountBalanceRepository
import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component

/**
 * Write-through on successful DynamoDB saveIfNewer; Redis put is version-aware (putIfNewer).
 */
@Component
@Primary
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class CachingAccountBalanceRepository(
    private val dynamoDbAccountBalanceRepository: DynamoDbAccountBalanceRepository,
    private val redisAccountBalanceCache: RedisAccountBalanceCache,
) : AccountBalanceRepository {
    override fun saveIfNewer(accountBalance: AccountBalance): Boolean {
        val saved = dynamoDbAccountBalanceRepository.saveIfNewer(accountBalance)
        // Always attempt cache update: version gate drops stale values; heals cache when
        // another writer already persisted the same/newer snapshot to DynamoDB.
        redisAccountBalanceCache.putIfNewer(accountBalance)
        return saved
    }
}
