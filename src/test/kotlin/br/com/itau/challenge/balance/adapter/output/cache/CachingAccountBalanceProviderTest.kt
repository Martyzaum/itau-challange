package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbAccountBalanceProvider
import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.mockito.BDDMockito.given
import org.mockito.BDDMockito.never
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CachingAccountBalanceProviderTest {
    private val dynamo = mock(DynamoDbAccountBalanceProvider::class.java)
    private val cache = mock(RedisAccountBalanceCache::class.java)
    private val registry = SimpleMeterRegistry()
    private val metrics = BalanceMetrics(registry)
    private val provider = CachingAccountBalanceProvider(dynamo, cache, metrics)

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val balance =
        AccountBalance(
            id = accountId,
            owner = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a"),
            balance = Balance(BigDecimal("1.00"), "BRL"),
            updatedAtMicros = 100L,
            lastTransactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
        )

    @Test
    fun `should return cached balance without hitting dynamodb`() {
        given(cache.get(accountId)).willReturn(balance)

        assertEquals(balance, provider.findByAccountId(accountId))
        verify(dynamo, never()).findByAccountId(accountId)
        assertEquals(1.0, registry.counter("balance.cache", "result", "hit").count())
    }

    @Test
    fun `should load dynamodb on miss and fill cache`() {
        given(cache.get(accountId)).willReturn(null)
        given(dynamo.findByAccountId(accountId)).willReturn(balance)

        assertEquals(balance, provider.findByAccountId(accountId))
        verify(cache).putIfNewer(balance)
        assertEquals(1.0, registry.counter("balance.cache", "result", "miss").count())
    }

    @Test
    fun `should return null when miss and dynamodb empty`() {
        given(cache.get(accountId)).willReturn(null)
        given(dynamo.findByAccountId(accountId)).willReturn(null)

        assertNull(provider.findByAccountId(accountId))
        verify(cache, never()).putIfNewer(balance)
    }
}
