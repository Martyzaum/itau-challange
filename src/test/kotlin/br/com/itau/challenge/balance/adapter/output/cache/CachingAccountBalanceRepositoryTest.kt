package br.com.itau.challenge.balance.adapter.output.cache

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.port.output.AccountBalanceCache
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.mockito.BDDMockito.given
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CachingAccountBalanceRepositoryTest {
    private val dynamo = mock(AccountBalanceRepository::class.java)
    private val cache = mock(AccountBalanceCache::class.java)
    private val metrics = BalanceMetrics(SimpleMeterRegistry())
    private val repository = CachingAccountBalanceRepository(dynamo, cache, metrics)

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
        given(cache.putIfNewer(balance)).willReturn(true)

        assertTrue(repository.saveIfNewer(balance))
        verify(cache).putIfNewer(balance)
        verify(cache, never()).invalidate(balance.id)
    }

    @Test
    fun `should not write rejected snapshot to cache when dynamodb save fails`() {
        given(dynamo.saveIfNewer(balance)).willReturn(false)

        assertFalse(repository.saveIfNewer(balance))
        verify(cache, never()).putIfNewer(balance)
        verify(cache, never()).invalidate(balance.id)
    }

    @Test
    fun `should invalidate cache when put fails after dynamodb save`() {
        given(dynamo.saveIfNewer(balance)).willReturn(true)
        given(cache.putIfNewer(balance)).willReturn(false)

        assertTrue(repository.saveIfNewer(balance))
        verify(cache).putIfNewer(balance)
        verify(cache).invalidate(balance.id)
    }
}
