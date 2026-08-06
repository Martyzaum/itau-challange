package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceCache
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Primary
import org.springframework.stereotype.Component
import java.util.UUID

@Component
@Primary
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class CachingAccountBalanceProvider(
    @Qualifier("dynamoDbAccountBalanceProvider")
    private val accountBalanceProvider: AccountBalanceProvider,
    private val accountBalanceCache: AccountBalanceCache,
    private val balanceMetrics: BalanceMetrics,
) : AccountBalanceProvider {
    override fun findByAccountId(accountId: UUID): AccountBalance? {
        val cached = accountBalanceCache.get(accountId)
        if (cached != null) {
            balanceMetrics.incrementCacheHit()
            return cached
        }
        balanceMetrics.incrementCacheMiss()
        val loaded = accountBalanceProvider.findByAccountId(accountId) ?: return null
        accountBalanceCache.putIfNewer(loaded)
        return loaded
    }
}
