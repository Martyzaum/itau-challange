package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import io.lettuce.core.api.sync.RedisCommands
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.eq
import org.mockito.BDDMockito.given
import org.mockito.BDDMockito.never
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RedisAccountBalanceCacheTest {
    private val commands: RedisCommands<String, String> = mock()
    private val objectMapper = JsonMapper.builder().addModule(kotlinModule()).build()
    private val cache =
        RedisAccountBalanceCache(
            commands = commands,
            objectMapper = objectMapper,
            keyPrefix = "balance:account:",
            ttl = Duration.ofSeconds(60),
        )

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val txId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")

    @Test
    fun `should return null on cache miss`() {
        given(commands.get("balance:account:$accountId")).willReturn(null)
        assertNull(cache.get(accountId))
    }

    @Test
    fun `should deserialize cached balance`() {
        val json =
            """{"id":"$accountId","owner":"$ownerId","amount":"183.12","currency":"BRL","updatedAtMicros":1751641364589998,"lastTransactionId":"$txId"}"""
        given(commands.get("balance:account:$accountId")).willReturn(json)

        val result = cache.get(accountId)!!
        assertEquals(accountId, result.id)
        assertEquals(BigDecimal("183.12"), result.balance.amount)
        assertEquals(1751641364589998L, result.updatedAtMicros)
        assertEquals(txId, result.lastTransactionId)
    }

    @Test
    fun `should fail open on redis get error`() {
        given(commands.get("balance:account:$accountId")).willThrow(RuntimeException("down"))
        assertNull(cache.get(accountId))
    }

    @Test
    fun `should put when cache empty`() {
        given(commands.get("balance:account:$accountId")).willReturn(null)

        cache.putIfNewer(sample(updatedAt = 100L, tx = txId))

        verify(commands).setex(eq("balance:account:$accountId"), eq(60L), anyString())
    }

    @Test
    fun `should skip put when cached version is newer`() {
        val newerTx = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        val cached =
            objectMapper.writeValueAsString(
                CachedAccountBalancePayload.from(sample(updatedAt = 200L, tx = newerTx)),
            )
        given(commands.get("balance:account:$accountId")).willReturn(cached)

        cache.putIfNewer(sample(updatedAt = 100L, tx = txId))

        verify(commands, never()).setex(anyString(), anyLong(), anyString())
        verify(commands, never()).set(anyString(), anyString())
    }

    @Test
    fun `should put when incoming version is newer`() {
        val olderTx = UUID.fromString("00000000-0000-4000-8000-000000000001")
        val cached =
            objectMapper.writeValueAsString(
                CachedAccountBalancePayload.from(sample(updatedAt = 100L, tx = olderTx)),
            )
        given(commands.get("balance:account:$accountId")).willReturn(cached)

        cache.putIfNewer(sample(updatedAt = 200L, tx = txId))

        verify(commands).setex(eq("balance:account:$accountId"), eq(60L), anyString())
    }

    private fun sample(
        updatedAt: Long,
        tx: UUID,
    ): AccountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = Balance(BigDecimal("10.00"), "BRL"),
            updatedAtMicros = updatedAt,
            lastTransactionId = tx,
        )
}
