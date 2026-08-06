package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import org.mockito.BDDMockito.given
import org.mockito.Mockito.mock
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RedisAccountBalanceCacheTest {
    private val objectMapper = JsonMapper.builder().addModule(kotlinModule()).build()

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val txId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")

    @Test
    fun `should return null on cache miss`() {
        val commands = mock(RedisCommands::class.java) as RedisCommands<String, String>
        given(commands.get("balance:account:$accountId")).willReturn(null)
        val cache = cache(commands)
        assertNull(cache.get(accountId))
    }

    @Test
    fun `should deserialize cached balance`() {
        val commands = mock(RedisCommands::class.java) as RedisCommands<String, String>
        val json =
            """{"id":"$accountId","owner":"$ownerId","amount":"183.12","currency":"BRL","updatedAtMicros":1751641364589998,"lastTransactionId":"$txId"}"""
        given(commands.get("balance:account:$accountId")).willReturn(json)

        val result = cache(commands).get(accountId)!!
        assertEquals(accountId, result.id)
        assertEquals(BigDecimal("183.12"), result.balance.amount)
        assertEquals(1751641364589998L, result.updatedAtMicros)
        assertEquals(txId, result.lastTransactionId)
    }

    @Test
    fun `should fail open on redis get error`() {
        val commands = mock(RedisCommands::class.java) as RedisCommands<String, String>
        given(commands.get("balance:account:$accountId")).willThrow(RuntimeException("down"))
        assertNull(cache(commands).get(accountId))
    }

    @Test
    fun `should putIfNewer via lua eval with version args`() {
        val recording = RecordingCommands()
        val balance = sample(updatedAt = 100L, tx = txId)
        val expectedPayload = objectMapper.writeValueAsString(CachedAccountBalancePayload.from(balance))

        cache(recording).putIfNewer(balance)

        assertTrue(recording.lastScript.contains("cjson.decode"))
        assertEquals(ScriptOutputType.INTEGER, recording.lastOutputType)
        assertEquals(listOf("balance:account:$accountId"), recording.lastKeys)
        assertEquals(
            listOf(expectedPayload, "100", txId.toString(), "60"),
            recording.lastArgs,
        )
    }

    @Test
    fun `should fail open on redis eval error`() {
        val failing =
            object : RecordingCommands() {
                override fun <T> eval(
                    script: String,
                    type: ScriptOutputType,
                    keys: Array<String>,
                    vararg values: String,
                ): T = throw RuntimeException("down")
            }

        cache(failing).putIfNewer(sample(updatedAt = 100L, tx = txId))
    }

    private fun cache(commands: RedisCommands<String, String>) =
        RedisAccountBalanceCache(
            commands = commands,
            objectMapper = objectMapper,
            keyPrefix = "balance:account:",
            ttl = Duration.ofSeconds(60),
        )

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

    private open class RecordingCommands : RedisCommands<String, String> by mock() {
        var lastScript: String = ""
        var lastOutputType: ScriptOutputType? = null
        var lastKeys: List<String> = emptyList()
        var lastArgs: List<String> = emptyList()

        override fun <T> eval(
            script: String,
            type: ScriptOutputType,
            keys: Array<String>,
            vararg values: String,
        ): T {
            lastScript = script
            lastOutputType = type
            lastKeys = keys.toList()
            lastArgs = values.toList()
            @Suppress("UNCHECKED_CAST")
            return 1L as T
        }
    }
}
