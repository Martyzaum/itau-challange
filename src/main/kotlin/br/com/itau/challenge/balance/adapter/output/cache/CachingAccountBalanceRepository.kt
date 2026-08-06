package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceCache
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import br.com.itau.challenge.balance.port.output.CachePutResult
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component

@Component
@Primary
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class CachingAccountBalanceRepository(
    @Qualifier("dynamoDbAccountBalanceRepository")
    private val accountBalanceRepository: AccountBalanceRepository,
    private val accountBalanceCache: AccountBalanceCache,
    private val balanceMetrics: BalanceMetrics,
) : AccountBalanceRepository {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun saveIfNewer(accountBalance: AccountBalance): Boolean {
        val saved = accountBalanceRepository.saveIfNewer(accountBalance)
        if (saved) {
            when (accountBalanceCache.putIfNewer(accountBalance)) {
                CachePutResult.WRITTEN, CachePutResult.REJECTED_NOT_NEWER -> Unit
                CachePutResult.FAILED -> {
                    balanceMetrics.incrementCachePutFailed()
                    logger.warn(
                        "event=balance_cache_put_failed_after_save accountId={} action=invalidate_if_not_newer",
                        accountBalance.id,
                    )
                    accountBalanceCache.invalidateIfNotNewer(accountBalance)
                }
            }
        }
        return saved
    }
}
