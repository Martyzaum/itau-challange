package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.adapter.output.redis.CachedAccountBalancePayload
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TABLE
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TOPIC
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.eligibleEventJson
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.math.BigDecimal
import java.net.URI
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
@AutoConfigureMockMvc
class BalanceCacheEndToEndIntegrationTest(
    @Autowired private val mockMvc: MockMvc,
    @Autowired private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    private val redisHost = System.getenv("BALANCE_CACHE_REDIS_HOST") ?: "localhost"
    private val redisPort = (System.getenv("BALANCE_CACHE_REDIS_PORT") ?: "6379").toInt()
    private val keyPrefix = "balance:account:"
    private val objectMapper = JsonMapper.builder().addModule(kotlinModule()).build()

    private lateinit var accountId: UUID
    private lateinit var ownerId: UUID
    private lateinit var redisClient: RedisClient
    private lateinit var redisConnection: StatefulRedisConnection<String, String>

    private val dynamoDbClient: DynamoDbClient =
        DynamoDbClient
            .builder()
            .endpointOverride(URI.create(System.getenv("DYNAMODB_ENDPOINT") ?: "http://localhost:8000"))
            .region(Region.of(System.getenv("DYNAMODB_REGION") ?: "us-east-1"))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
            .build()

    @BeforeEach
    fun setUp() {
        accountId = UUID.randomUUID()
        ownerId = UUID.randomUUID()
        redisClient =
            RedisClient.create(
                RedisURI.Builder
                    .redis(redisHost, redisPort)
                    .withTimeout(Duration.ofSeconds(2))
                    .build(),
            )
        redisConnection = redisClient.connect()
        redisConnection.sync().del(cacheKey())
        // Let the Kafka listener finish partition assignment when using auto-offset-reset=latest.
        Thread.sleep(1_500)
    }

    @AfterEach
    fun tearDown() {
        runCatching { redisConnection.sync().del(cacheKey()) }
        runCatching { redisConnection.close() }
        runCatching { redisClient.shutdown() }
        dynamoDbClient.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(TABLE)
                .key(mapOf("account_id" to AttributeValue.builder().s(accountId.toString()).build()))
                .build(),
        )
    }

    @Test
    fun `should populate redis on kafka ingest and serve balance via rest`() {
        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("183.12"),
                timestampMicros = 1_751_641_364_589_998L,
            ),
        )

        awaitBalance(expectedAmount = 183.12)
        awaitCacheAmount(expectedAmount = "183.12")
    }

    @Test
    fun `should serve from redis hit after dynamodb row is deleted`() {
        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("99.50"),
                timestampMicros = 1_751_641_364_589_998L,
            ),
        )
        awaitBalance(expectedAmount = 99.50)
        awaitCacheAmount(expectedAmount = "99.50")

        // Warm path already filled cache via write-through; force another GET for hit metrics path
        assertBalance(expectedAmount = 99.50)

        dynamoDbClient.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(TABLE)
                .key(mapOf("account_id" to AttributeValue.builder().s(accountId.toString()).build()))
                .build(),
        )

        // Cache-aside must still return the snapshot from Redis
        assertBalance(expectedAmount = 99.50)
        assertNotNull(readCachePayload())
    }

    @Test
    fun `should refresh redis when a newer event arrives`() {
        val olderTs = 1_751_641_364_589_998L
        val newerTs = olderTs + 1_000_000L

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("10.00"),
                timestampMicros = olderTs,
            ),
        )
        awaitBalance(expectedAmount = 10.00)
        awaitCacheAmount(expectedAmount = "10.00")

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("77.77"),
                timestampMicros = newerTs,
            ),
        )
        awaitBalance(expectedAmount = 77.77)
        awaitCacheAmount(expectedAmount = "77.77")

        val cached = assertNotNull(readCachePayload())
        assertEquals(newerTs, cached.updatedAtMicros)
    }

    @Test
    fun `should keep newer redis value when older event is ignored`() {
        val newerTs = 1_751_641_364_589_998L
        val olderTs = newerTs - 1_000_000L

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("200.00"),
                timestampMicros = newerTs,
            ),
        )
        awaitBalance(expectedAmount = 200.00)
        awaitCacheAmount(expectedAmount = "200.00")

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("1.00"),
                timestampMicros = olderTs,
            ),
        )

        // REST + cache stay on newer snapshot
        Thread.sleep(1_500)
        assertBalance(expectedAmount = 200.00)
        awaitCacheAmount(expectedAmount = "200.00")
        assertEquals(newerTs, assertNotNull(readCachePayload()).updatedAtMicros)
    }

    private fun publish(payload: String) {
        kafkaTemplate
            .send(TOPIC, accountId.toString(), payload)
            .get(10, TimeUnit.SECONDS)
    }

    private fun cacheKey(): String = "$keyPrefix$accountId"

    private fun readCachePayload(): CachedAccountBalancePayload? {
        val raw = redisConnection.sync().get(cacheKey()) ?: return null
        return objectMapper.readValue(raw, CachedAccountBalancePayload::class.java)
    }

    private fun awaitCacheAmount(
        expectedAmount: String,
        timeoutSeconds: Long = 20,
    ) {
        val expected = BigDecimal(expectedAmount)
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        var last: CachedAccountBalancePayload? = null
        while (System.nanoTime() < deadline) {
            last = readCachePayload()
            if (last != null && expected.compareTo(BigDecimal(last.amount)) == 0) {
                return
            }
            Thread.sleep(200)
        }
        throw AssertionError("Redis cache for $accountId did not reach amount=$expectedAmount (last=$last)")
    }

    private fun awaitBalance(
        expectedAmount: Double,
        timeoutSeconds: Long = 20,
    ) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        var lastError: Throwable? = null
        while (System.nanoTime() < deadline) {
            try {
                assertBalance(expectedAmount)
                return
            } catch (error: Throwable) {
                lastError = error
                Thread.sleep(250)
            }
        }
        throw AssertionError("Balance was not available via REST within timeout", lastError)
    }

    private fun assertBalance(expectedAmount: Double) {
        mockMvc.get("/balances/$accountId").andExpect {
            status { isOk() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.id") { value(accountId.toString()) }
            jsonPath("$.owner") { value(ownerId.toString()) }
            jsonPath("$.balance.amount") { value(expectedAmount) }
            jsonPath("$.balance.currency") { value("BRL") }
        }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun cacheProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.consumer.group-id") {
                "balance-cache-e2e-${UUID.randomUUID()}"
            }
            // Avoid replaying the whole topic (load-test backlog) before our events.
            registry.add("spring.kafka.consumer.auto-offset-reset") { "latest" }
            registry.add("balance.cache.enabled") { "true" }
            registry.add("balance.cache.redis.host") {
                System.getenv("BALANCE_CACHE_REDIS_HOST") ?: "localhost"
            }
            registry.add("balance.cache.redis.port") {
                System.getenv("BALANCE_CACHE_REDIS_PORT") ?: "6379"
            }
            registry.add("balance.cache.ttl-seconds") { "300" }
            registry.add("management.otlp.metrics.export.enabled") { "false" }
            registry.add("management.tracing.enabled") { "false" }
            registry.add("management.logging.export.otlp.enabled") { "false" }
        }
    }
}
