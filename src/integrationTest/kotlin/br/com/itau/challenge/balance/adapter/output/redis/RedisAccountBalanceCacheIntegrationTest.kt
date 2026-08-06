package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class RedisAccountBalanceCacheIntegrationTest {
    private val host = System.getenv("BALANCE_CACHE_REDIS_HOST") ?: System.getenv("REDIS_HOST") ?: "localhost"
    private val port = (System.getenv("BALANCE_CACHE_REDIS_PORT") ?: System.getenv("REDIS_PORT") ?: "6379").toInt()
    private val keyPrefix = "balance:itest:account:"

    private lateinit var client: RedisClient
    private lateinit var connection: StatefulRedisConnection<String, String>
    private lateinit var cache: RedisAccountBalanceCache
    private lateinit var accountId: UUID

    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val objectMapper = JsonMapper.builder().addModule(kotlinModule()).build()

    @BeforeEach
    fun setUp() {
        accountId = UUID.randomUUID()
        client =
            RedisClient.create(
                RedisURI.Builder
                    .redis(host, port)
                    .withTimeout(Duration.ofSeconds(2))
                    .build(),
            )
        connection = client.connect()
        cache =
            RedisAccountBalanceCache(
                commands = connection.sync(),
                objectMapper = objectMapper,
                keyPrefix = keyPrefix,
                ttl = Duration.ofSeconds(120),
                circuitBreaker = io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults()
                    .circuitBreaker("redis"),
            )
    }

    @AfterEach
    fun tearDown() {
        runCatching { connection.sync().del("$keyPrefix$accountId") }
        runCatching { connection.close() }
        runCatching { client.shutdown() }
    }

    @Test
    fun `should round trip balance through redis`() {
        val balance = accountBalance(updatedAtMicros = 100, amount = "183.12")
        cache.putIfNewer(balance)

        val loaded = assertNotNull(cache.get(accountId))
        assertEquals(accountId, loaded.id)
        assertEquals(0, BigDecimal("183.12").compareTo(loaded.balance.amount))
        assertEquals(100L, loaded.updatedAtMicros)
        assertEquals(balance.lastTransactionId, loaded.lastTransactionId)
    }

    @Test
    fun `should not overwrite newer cached version with older putIfNewer`() {
        val newerTx = UUID.fromString("ffffffff-ffff-4fff-8fff-ffffffffffff")
        val olderTx = UUID.fromString("00000000-0000-4000-8000-000000000001")

        cache.putIfNewer(accountBalance(updatedAtMicros = 200, amount = "200.00", lastTransactionId = newerTx))
        cache.putIfNewer(accountBalance(updatedAtMicros = 100, amount = "50.00", lastTransactionId = olderTx))

        val loaded = assertNotNull(cache.get(accountId))
        assertEquals(200L, loaded.updatedAtMicros)
        assertEquals(0, BigDecimal("200.00").compareTo(loaded.balance.amount))
        assertEquals(newerTx, loaded.lastTransactionId)
    }

    @Test
    fun `should accept higher transaction id on equal timestamp`() {
        val lowerTx = UUID.fromString("00000000-0000-4000-8000-000000000001")
        val higherTx = UUID.fromString("ffffffff-ffff-4fff-8fff-ffffffffffff")

        cache.putIfNewer(accountBalance(updatedAtMicros = 200, amount = "100.00", lastTransactionId = lowerTx))
        cache.putIfNewer(accountBalance(updatedAtMicros = 200, amount = "250.00", lastTransactionId = higherTx))

        val loaded = assertNotNull(cache.get(accountId))
        assertEquals(higherTx, loaded.lastTransactionId)
        assertEquals(0, BigDecimal("250.00").compareTo(loaded.balance.amount))
    }

    @Test
    fun `should keep newest snapshot when concurrent workers race older versions`() {
        val newestTx = UUID.fromString("ffffffff-ffff-4fff-8fff-ffffffffffff")
        cache.putIfNewer(
            accountBalance(updatedAtMicros = 500, amount = "500.00", lastTransactionId = newestTx),
        )

        val executor = Executors.newFixedThreadPool(8)
        try {
            val tasks =
                (1..40).map { offset ->
                    Callable {
                        val workerClient =
                            RedisClient.create(
                                RedisURI.Builder
                                    .redis(host, port)
                                    .withTimeout(Duration.ofSeconds(2))
                                    .build(),
                            )
                        try {
                            workerClient.connect().use { workerConn ->
                                val workerCache =
                                    RedisAccountBalanceCache(
                commands = workerConn.sync(),
                objectMapper = objectMapper,
                keyPrefix = keyPrefix,
                ttl = Duration.ofSeconds(120),
                circuitBreaker = io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults()
                    .circuitBreaker("redis"),
            )
                                workerCache.putIfNewer(
                                    accountBalance(
                                        updatedAtMicros = offset.toLong(),
                                        amount = "$offset.00",
                                        lastTransactionId = UUID.randomUUID(),
                                    ),
                                )
                            }
                        } finally {
                            workerClient.shutdown()
                        }
                    }
                }
            executor.invokeAll(tasks).forEach { it.get(15, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }

        val loaded = assertNotNull(cache.get(accountId))
        assertEquals(500L, loaded.updatedAtMicros)
        assertEquals(0, BigDecimal("500.00").compareTo(loaded.balance.amount))
        assertEquals(newestTx, loaded.lastTransactionId)
    }

    @Test
    fun `should fail open when redis commands error`() {
        val broken =
            object : RedisCommands<String, String> by mock() {
                override fun get(key: String): String = throw RuntimeException("down")

                override fun <T> eval(
                    script: String,
                    type: ScriptOutputType,
                    keys: Array<String>,
                    vararg values: String,
                ): T = throw RuntimeException("down")
            }

        val brokenCache =
            RedisAccountBalanceCache(
                commands = broken,
                objectMapper = objectMapper,
                keyPrefix = keyPrefix,
                ttl = Duration.ofSeconds(30),
                circuitBreaker = io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults()
                    .circuitBreaker("redis"),
            )

        assertNull(brokenCache.get(accountId))
        brokenCache.putIfNewer(accountBalance(updatedAtMicros = 1, amount = "1.00"))
    }

    private fun accountBalance(
        updatedAtMicros: Long,
        amount: String,
        lastTransactionId: UUID = UUID.randomUUID(),
    ): AccountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = Balance(BigDecimal(amount), "BRL"),
            updatedAtMicros = updatedAtMicros,
            lastTransactionId = lastTransactionId,
        )
}
