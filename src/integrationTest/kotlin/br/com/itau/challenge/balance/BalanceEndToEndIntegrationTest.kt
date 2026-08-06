package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.DLT_TOPIC
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TABLE
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TOPIC
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.eligibleEventJson
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.invalidDomainPayload
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.invalidJsonPayload
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
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
import java.math.BigDecimal
import java.net.URI
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Full path: publish Kafka event → consumer persists to DynamoDB → GET /balances/{accountId}.
 * Requires live DynamoDB Local + Redpanda (`make db-up` and `make kafka-up`).
 */
@SpringBootTest
@AutoConfigureMockMvc
class BalanceEndToEndIntegrationTest(
    @Autowired private val mockMvc: MockMvc,
    @Autowired private val kafkaTemplate: KafkaTemplate<String, String>,
    @Autowired private val consumerFactory: ConsumerFactory<String, String>,
) {

    private lateinit var accountId: UUID
    private lateinit var ownerId: UUID

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
    }

    @AfterEach
    fun tearDown() {
        dynamoDbClient.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(TABLE)
                .key(mapOf("account_id" to AttributeValue.builder().s(accountId.toString()).build()))
                .build(),
        )
    }

    @Test
    fun `should ingest kafka event persist balance and expose it via rest`() {
        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("183.12"),
                timestampMicros = 1_751_641_364_589_998L,
            ),
        )

        awaitBalance(expectedOwnerId = ownerId, expectedAmount = 183.12)
    }

    @Test
    fun `should keep newer balance when an older event arrives out of order`() {
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
        awaitBalance(expectedOwnerId = ownerId, expectedAmount = 200.00)

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("50.00"),
                timestampMicros = olderTs,
            ),
        )

        awaitBalanceStable(
            expectedOwnerId = ownerId,
            expectedAmount = 200.00,
            stableForMs = 1_500,
        )
    }

    @Test
    fun `should ignore redelivered event with same timestamp and transaction id`() {
        val timestamp = 1_751_641_364_589_998L
        val transactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                transactionId = transactionId,
                balanceAmount = BigDecimal("183.12"),
                timestampMicros = timestamp,
            ),
        )
        awaitBalance(expectedOwnerId = ownerId, expectedAmount = 183.12)

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                transactionId = transactionId,
                balanceAmount = BigDecimal("999.99"),
                timestampMicros = timestamp,
            ),
        )

        awaitBalanceStable(
            expectedOwnerId = ownerId,
            expectedAmount = 183.12,
            stableForMs = 1_500,
        )
    }

    @Test
    fun `should apply distinct transaction when timestamps tie using transaction id order`() {
        val timestamp = 1_751_641_364_589_998L
        val lowerTxId = UUID.fromString("00000000-0000-4000-8000-000000000001")
        val higherTxId = UUID.fromString("ffffffff-ffff-4fff-8fff-ffffffffffff")

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                transactionId = lowerTxId,
                balanceAmount = BigDecimal("100.00"),
                timestampMicros = timestamp,
            ),
        )
        awaitBalance(expectedOwnerId = ownerId, expectedAmount = 100.00)

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                transactionId = higherTxId,
                balanceAmount = BigDecimal("250.00"),
                timestampMicros = timestamp,
            ),
        )

        awaitBalance(expectedOwnerId = ownerId, expectedAmount = 250.00)
    }

    @Test
    fun `should not persist declined transaction`() {
        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("183.12"),
                transactionStatus = "DECLINED",
                timestampMicros = 1_751_641_364_589_998L,
            ),
        )

        awaitNotFound(stableForMs = 2_000)
    }

    @Test
    fun `should not overwrite balance when account is disabled`() {
        val enabledTs = 1_751_641_364_589_998L

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("183.12"),
                timestampMicros = enabledTs,
            ),
        )
        awaitBalance(expectedOwnerId = ownerId, expectedAmount = 183.12)

        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("10.00"),
                accountStatus = "DISABLED",
                timestampMicros = enabledTs + 1_000_000L,
            ),
        )

        awaitBalanceStable(
            expectedOwnerId = ownerId,
            expectedAmount = 183.12,
            stableForMs = 1_500,
        )
    }

    @Test
    fun `should send invalid json payload to dlt without persisting balance`() {
        val payload = invalidJsonPayload()
        val dltRecord =
            withDltConsumerAtEnd { consumer ->
                publish(payload)
                awaitDltRecord(consumer, expectedValue = payload)
            }

        assertEquals(accountId.toString(), dltRecord.key())
        assertEquals(payload, dltRecord.value())
        assertDltOriginalTopic(dltRecord)
        awaitNotFound(stableForMs = 1_000, timeoutSeconds = 2)
    }

    @Test
    fun `should send invalid domain payload to dlt without persisting balance`() {
        val payload = invalidDomainPayload(accountId = accountId, ownerId = ownerId)
        val dltRecord =
            withDltConsumerAtEnd { consumer ->
                publish(payload)
                awaitDltRecord(consumer, expectedValue = payload)
            }

        assertEquals(accountId.toString(), dltRecord.key())
        assertEquals(payload, dltRecord.value())
        assertDltOriginalTopic(dltRecord)
        awaitNotFound(stableForMs = 1_000, timeoutSeconds = 2)
    }

    private fun publish(payload: String) {
        kafkaTemplate
            .send(TOPIC, accountId.toString(), payload)
            .get(10, TimeUnit.SECONDS)
    }

    private fun <T> withDltConsumerAtEnd(block: (Consumer<String, String>) -> T): T {
        val overrides =
            Properties().apply {
                setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
            }
        val consumer =
            consumerFactory.createConsumer(
                "balance-e2e-dlt-${UUID.randomUUID()}",
                "dlt-assert",
                null,
                overrides,
            )
        consumer.use {
            it.subscribe(listOf(DLT_TOPIC))
            seekToEnd(it)
            return block(it)
        }
    }

    private fun seekToEnd(consumer: Consumer<String, String>) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(100))
        }
        val assignment = consumer.assignment()
        check(assignment.isNotEmpty()) { "DLT consumer was not assigned partitions for $DLT_TOPIC" }
        val endOffsets = consumer.endOffsets(assignment)
        assignment.forEach { partition: TopicPartition ->
            consumer.seek(partition, endOffsets.getValue(partition))
        }
    }

    private fun awaitDltRecord(
        consumer: Consumer<String, String>,
        expectedValue: String,
        timeoutSeconds: Long = 15,
    ): ConsumerRecord<String, String> {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        while (System.nanoTime() < deadline) {
            val records = consumer.poll(Duration.ofMillis(500))
            records.records(DLT_TOPIC).forEach { record ->
                if (record.key() == accountId.toString() && record.value() == expectedValue) {
                    return record
                }
            }
        }
        throw AssertionError("DLT record for account $accountId was not received within timeout")
    }

    private fun assertDltOriginalTopic(record: ConsumerRecord<String, String>) {
        val originalTopicHeader = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
        assertNotNull(originalTopicHeader, "missing ${KafkaHeaders.DLT_ORIGINAL_TOPIC} header")
        assertEquals(TOPIC, String(originalTopicHeader.value()))
        val exceptionHeader =
            record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)
                ?: record.headers().lastHeader("kafka_dlt-exception-fqcn")
        assertNotNull(exceptionHeader, "missing DLT exception class header")
        assertTrue(String(exceptionHeader.value()).isNotBlank())
    }

    private fun awaitBalance(
        expectedOwnerId: UUID,
        expectedAmount: Double,
        currency: String = "BRL",
        timeoutSeconds: Long = 20,
    ) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        var lastError: Throwable? = null
        while (System.nanoTime() < deadline) {
            try {
                assertBalance(expectedOwnerId, expectedAmount, currency)
                return
            } catch (error: Throwable) {
                lastError = error
                Thread.sleep(250)
            }
        }
        throw AssertionError("Balance was not available via REST within timeout", lastError)
    }

    private fun awaitBalanceStable(
        expectedOwnerId: UUID,
        expectedAmount: Double,
        stableForMs: Long,
        currency: String = "BRL",
        timeoutSeconds: Long = 20,
    ) {
        awaitBalance(expectedOwnerId, expectedAmount, currency, timeoutSeconds)
        val stableDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(stableForMs)
        while (System.nanoTime() < stableDeadline) {
            assertBalance(expectedOwnerId, expectedAmount, currency)
            Thread.sleep(250)
        }
    }

    private fun awaitNotFound(stableForMs: Long, timeoutSeconds: Long = 5) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        while (System.nanoTime() < deadline) {
            mockMvc.get("/balances/$accountId").andExpect {
                status { isNotFound() }
            }
            Thread.sleep(250)
        }
        val stableDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(stableForMs)
        while (System.nanoTime() < stableDeadline) {
            mockMvc.get("/balances/$accountId").andExpect {
                status { isNotFound() }
            }
            Thread.sleep(250)
        }
    }

    private fun assertBalance(
        expectedOwnerId: UUID,
        expectedAmount: Double,
        currency: String,
    ) {
        mockMvc.get("/balances/$accountId").andExpect {
            status { isOk() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.id") { value(accountId.toString()) }
            jsonPath("$.owner") { value(expectedOwnerId.toString()) }
            jsonPath("$.balance.amount") { value(expectedAmount) }
            jsonPath("$.balance.currency") { value(currency) }
            jsonPath("$.updated_at") { exists() }
        }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun kafkaConsumerGroup(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.consumer.group-id") {
                "balance-e2e-${UUID.randomUUID()}"
            }
            registry.add("management.otlp.metrics.export.enabled") { "false" }
            registry.add("management.otlp.tracing.export.enabled") { "false" }
            registry.add("management.tracing.enabled") { "false" }
        }
    }
}
