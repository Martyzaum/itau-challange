package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.adapter.input.kafka.KafkaRetryHeaders
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import br.com.itau.challenge.balance.support.AwaitilitySupport.awaitAtMost
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.DLT_TOPIC
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TOPIC
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.eligibleEventJson
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private const val RETRY_1 = "transacoes-financeiras-processadas.retry-1"

@SpringBootTest
@Import(AsyncRetryEndToEndIntegrationTest.FailingRepositoryConfig::class)
class AsyncRetryEndToEndIntegrationTest(
    @Autowired private val kafkaTemplate: KafkaTemplate<String, String>,
    @Autowired private val consumerFactory: ConsumerFactory<String, String>,
    @Autowired private val listenerRegistry: KafkaListenerEndpointRegistry,
) {
    private lateinit var accountId: UUID
    private lateinit var ownerId: UUID

    @BeforeEach
    fun setUp() {
        accountId = UUID.randomUUID()
        ownerId = UUID.randomUUID()
        awaitListenersAssigned()
    }

    private fun awaitListenersAssigned() {
        awaitAtMost(30).until {
            val containers = listenerRegistry.listenerContainers
            containers.isNotEmpty() &&
                containers.all { container ->
                    container.isRunning && !container.assignedPartitions.isNullOrEmpty()
                }
        }
    }

    @Test
    fun `should route technical failure from main topic to retry-1 with attempt header`() {
        val payload =
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("183.12"),
                timestampMicros = 1_751_641_364_589_998L,
            )

        val retryRecord =
            withTopicConsumerAtEnd(RETRY_1) { consumer ->
                kafkaTemplate
                    .send(TOPIC, accountId.toString(), payload)
                    .get(10, TimeUnit.SECONDS)
                awaitRecord(consumer, topic = RETRY_1, expectedValue = payload)
            }

        assertEquals(accountId.toString(), retryRecord.key())
        assertEquals(payload, retryRecord.value())
        val attemptHeader = retryRecord.headers().lastHeader(KafkaRetryHeaders.RETRY_ATTEMPT)
        assertNotNull(attemptHeader)
        assertEquals("1", String(attemptHeader.value(), StandardCharsets.UTF_8))
        assertNotNull(retryRecord.headers().lastHeader(KafkaRetryHeaders.RETRY_FAILED_AT_MS))
        val originalTopic = retryRecord.headers().lastHeader(KafkaRetryHeaders.ORIGINAL_TOPIC)
        assertNotNull(originalTopic)
        assertEquals(TOPIC, String(originalTopic.value(), StandardCharsets.UTF_8))
    }

    @Test
    fun `should route to DLT after retry attempts are exhausted`() {
        val payload =
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                balanceAmount = BigDecimal("42.00"),
                timestampMicros = 1_751_641_364_589_998L,
            )

        val dltRecord =
            withTopicConsumerAtEnd(DLT_TOPIC) { consumer ->
                kafkaTemplate
                    .send(TOPIC, accountId.toString(), payload)
                    .get(10, TimeUnit.SECONDS)
                // max-attempts=3 → hops main→retry-1→2→3→DLT (non-blocking)
                awaitRecord(consumer, topic = DLT_TOPIC, expectedValue = payload, timeoutSeconds = 45)
            }

        assertEquals(accountId.toString(), dltRecord.key())
        assertEquals(payload, dltRecord.value())
        val attemptHeader = dltRecord.headers().lastHeader(KafkaRetryHeaders.RETRY_ATTEMPT)
        assertNotNull(attemptHeader)
        val attempt = String(attemptHeader.value(), StandardCharsets.UTF_8).toInt()
        assertTrue(attempt >= 3, "expected attempt >= 3 on exhausted path, was $attempt")
        val originalTopic =
            dltRecord.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
                ?: dltRecord.headers().lastHeader(KafkaRetryHeaders.ORIGINAL_TOPIC)
        assertNotNull(originalTopic)
        val exceptionHeader =
            dltRecord.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)
                ?: dltRecord.headers().lastHeader("kafka_dlt-exception-fqcn")
        assertNotNull(exceptionHeader, "missing DLT exception class header")
    }

    private fun <T> withTopicConsumerAtEnd(
        topic: String,
        block: (Consumer<String, String>) -> T,
    ): T {
        val overrides =
            Properties().apply {
                setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
            }
        val consumer =
            consumerFactory.createConsumer(
                "balance-async-retry-e2e-${UUID.randomUUID()}",
                "assert-$topic",
                null,
                overrides,
            )
        consumer.use {
            it.subscribe(listOf(topic))
            seekToEnd(it, topic)
            it.poll(Duration.ofMillis(200))
            return block(it)
        }
    }

    private fun seekToEnd(
        consumer: Consumer<String, String>,
        topic: String,
    ) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(100))
        }
        val assignment = consumer.assignment()
        check(assignment.isNotEmpty()) { "consumer was not assigned partitions for $topic" }
        val endOffsets = consumer.endOffsets(assignment)
        assignment.forEach { partition: TopicPartition ->
            consumer.seek(partition, endOffsets.getValue(partition))
        }
    }

    private fun awaitRecord(
        consumer: Consumer<String, String>,
        topic: String,
        expectedValue: String,
        timeoutSeconds: Long = 20,
    ): ConsumerRecord<String, String> {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        while (System.nanoTime() < deadline) {
            val records = consumer.poll(Duration.ofMillis(500))
            records.records(topic).forEach { record ->
                if (record.key() == accountId.toString() && record.value() == expectedValue) {
                    return record
                }
            }
        }
        throw AssertionError("$topic record for account $accountId was not received within ${timeoutSeconds}s")
    }

    @TestConfiguration
    class FailingRepositoryConfig {
        @Bean
        @Primary
        fun failingAccountBalanceRepository(): AccountBalanceRepository =
            AccountBalanceRepository {
                throw IllegalStateException("dynamodb unavailable")
            }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun props(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.consumer.group-id") {
                "balance-async-retry-e2e-${UUID.randomUUID()}"
            }
            registry.add("spring.kafka.consumer.auto-offset-reset") { "latest" }
            registry.add("transactions.retry.max-attempts") { "3" }
            // keep hops fast in CI; production defaults use exp backoff + jitter
            registry.add("transactions.retry.initial-interval-ms") { "0" }
            registry.add("transactions.retry.max-interval-ms") { "0" }
            registry.add("spring.kafka.listener.concurrency") { "1" }
            registry.add("balance.cache.enabled") { "false" }
            registry.add("api.auth.enabled") { "false" }
            registry.add("management.otlp.metrics.export.enabled") { "false" }
            registry.add("management.tracing.enabled") { "false" }
            registry.add("management.logging.export.otlp.enabled") { "false" }
        }
    }
}
