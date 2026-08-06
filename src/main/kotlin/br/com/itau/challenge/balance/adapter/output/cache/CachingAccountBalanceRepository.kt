package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbAccountBalanceRepository
import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component

@Component
@Primary
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class CachingAccountBalanceRepository(
    private val dynamoDbAccountBalanceRepository: DynamoDbAccountBalanceRepository,
    private val redisAccountBalanceCache: RedisAccountBalanceCache,
    private val balanceMetrics: BalanceMetrics,
) : AccountBalanceRepository {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun saveIfNewer(accountBalance: AccountBalance): Boolean {
        val saved = dynamoDbAccountBalanceRepository.saveIfNewer(accountBalance)
        if (saved) {
            val cached = redisAccountBalanceCache.putIfNewer(accountBalance)
            if (!cached) {
                balanceMetrics.incrementCachePutFailed()
                logger.warn(
                    "event=balance_cache_put_failed_after_save accountId={} action=invalidate",
                    accountBalance.id,
                )
                redisAccountBalanceCache.invalidate(accountBalance.id)
            }
        }
        return saved
    }
}
