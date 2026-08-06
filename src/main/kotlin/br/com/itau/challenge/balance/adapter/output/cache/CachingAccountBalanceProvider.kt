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
import java.util.concurrent.ConcurrentHashMap

@Component
@Primary
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class CachingAccountBalanceProvider(
    @Qualifier("dynamoDbAccountBalanceProvider")
    private val accountBalanceProvider: AccountBalanceProvider,
    private val accountBalanceCache: AccountBalanceCache,
    private val balanceMetrics: BalanceMetrics,
) : AccountBalanceProvider {
    private val loadLocks = ConcurrentHashMap<UUID, Any>()

    override fun findByAccountId(accountId: UUID): AccountBalance? {
        accountBalanceCache.get(accountId)?.let {
            balanceMetrics.incrementCacheHit()
            return it
        }
        balanceMetrics.incrementCacheMiss()

        val lock = loadLocks.computeIfAbsent(accountId) { Any() }
        try {
            synchronized(lock) {
                accountBalanceCache.get(accountId)?.let {
                    return it
                }
                val loaded = accountBalanceProvider.findByAccountId(accountId) ?: return null
                accountBalanceCache.putIfNewer(loaded)
                return loaded
            }
        } finally {
            loadLocks.remove(accountId, lock)
        }
    }
}
