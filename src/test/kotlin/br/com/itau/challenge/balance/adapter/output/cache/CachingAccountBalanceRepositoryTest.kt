package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.output.dynamodb.DynamoDbAccountBalanceRepository
import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import org.mockito.BDDMockito.given
import org.mockito.BDDMockito.never
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CachingAccountBalanceRepositoryTest {
    private val dynamo = mock(DynamoDbAccountBalanceRepository::class.java)
    private val cache = mock(RedisAccountBalanceCache::class.java)
    private val repository = CachingAccountBalanceRepository(dynamo, cache)

    private val balance =
        AccountBalance(
            id = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"),
            owner = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a"),
            balance = Balance(BigDecimal("1.00"), "BRL"),
            updatedAtMicros = 100L,
            lastTransactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
        )

    @Test
    fun `should update cache when dynamodb save succeeds`() {
        given(dynamo.saveIfNewer(balance)).willReturn(true)

        assertTrue(repository.saveIfNewer(balance))
        verify(cache).putIfNewer(balance)
    }

    @Test
    fun `should not touch cache when dynamodb save is rejected`() {
        given(dynamo.saveIfNewer(balance)).willReturn(false)

        assertFalse(repository.saveIfNewer(balance))
        verify(cache, never()).putIfNewer(balance)
    }
}
