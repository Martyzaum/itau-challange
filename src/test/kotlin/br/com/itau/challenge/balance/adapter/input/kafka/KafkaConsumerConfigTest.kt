package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.application.exception.DependencyUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidAccountBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.config.CircuitBreakerNames
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import tools.jackson.core.JacksonException
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KafkaConsumerConfigTest {

    private fun metrics(): BalanceMetrics = BalanceMetrics(SimpleMeterRegistry())

    private fun retryProps() =
        TransactionRetryProperties(
            maxAttempts = 3,
            initialIntervalMs = 1_000,
            multiplier = 2.0,
            maxIntervalMs = 30_000,
        )

    @Test
    fun `should classify payload errors as not retryable and technical errors as retryable`() {
        val notRetryable = notRetryableExceptionTypes().toSet()

        assertTrue(JacksonException::class.java in notRetryable)
        assertTrue(InvalidTransactionEventException::class.java in notRetryable)
        assertTrue(InvalidBalanceException::class.java in notRetryable)
        assertTrue(InvalidAccountBalanceException::class.java in notRetryable)
        assertFalse(IllegalArgumentException::class.java in notRetryable)
        assertFalse(NullPointerException::class.java in notRetryable)
        assertFalse(IllegalStateException::class.java in notRetryable)
        assertFalse(RuntimeException::class.java in notRetryable)
        assertTrue(isNotRetryable(InvalidTransactionEventException("bad")))
        assertFalse(isNotRetryable(NullPointerException("missing field")))
        assertFalse(isNotRetryable(IllegalStateException("down")))
    }

    @Test
    fun `should route not retryable failures straight to dlt`() {
        val record =
            ConsumerRecord(
                "transacoes-financeiras-processadas",
                2,
                15L,
                "account-1",
                """{"bad":true}""",
            )

        val destination =
            resolveFailureDestination(
                record = record,
                exception = InvalidTransactionEventException("bad"),
                retryTopics =
                    listOf(
                        "transacoes-financeiras-processadas.retry-1",
                        "transacoes-financeiras-processadas.retry-2",
                        "transacoes-financeiras-processadas.retry-3",
                    ),
                dltTopicName = "transacoes-financeiras-processadas.DLT",
            )

        assertEquals("transacoes-financeiras-processadas.DLT", destination.topic())
        assertEquals(2, destination.partition())
    }

    @Test
    fun `should route technical failure from main to retry-1`() {
        val record =
            ConsumerRecord(
                "transacoes-financeiras-processadas",
                1,
                10L,
                "account-1",
                "{}",
            )

        val destination =
            resolveFailureDestination(
                record = record,
                exception = IllegalStateException("dynamo down"),
                retryTopics =
                    listOf(
                        "transacoes-financeiras-processadas.retry-1",
                        "transacoes-financeiras-processadas.retry-2",
                        "transacoes-financeiras-processadas.retry-3",
                    ),
                dltTopicName = "transacoes-financeiras-processadas.DLT",
            )

        assertEquals("transacoes-financeiras-processadas.retry-1", destination.topic())
        assertEquals(1, destination.partition())
    }

    @Test
    fun `should route technical failure from retry-3 to dlt`() {
        val record =
            ConsumerRecord(
                "transacoes-financeiras-processadas.retry-3",
                0,
                10L,
                "account-1",
                "{}",
            )
        record.headers().add(
            RecordHeader(
                KafkaRetryHeaders.RETRY_ATTEMPT,
                "3".toByteArray(StandardCharsets.UTF_8),
            ),
        )

        val destination =
            resolveFailureDestination(
                record = record,
                exception = IllegalStateException("still down"),
                retryTopics =
                    listOf(
                        "transacoes-financeiras-processadas.retry-1",
                        "transacoes-financeiras-processadas.retry-2",
                        "transacoes-financeiras-processadas.retry-3",
                    ),
                dltTopicName = "transacoes-financeiras-processadas.DLT",
            )

        assertEquals("transacoes-financeiras-processadas.DLT", destination.topic())
    }

    @Test
    fun `should build default error handler with zero in-process backoff`() {
        @Suppress("UNCHECKED_CAST")
        val kafkaOperations = mock(KafkaOperations::class.java) as KafkaOperations<String, String>

        val errorHandler =
            createKafkaErrorHandler(
                kafkaOperations = kafkaOperations,
                dltTopicName = "transacoes-financeiras-processadas.DLT",
                retryTopics =
                    listOf(
                        "transacoes-financeiras-processadas.retry-1",
                        "transacoes-financeiras-processadas.retry-2",
                        "transacoes-financeiras-processadas.retry-3",
                    ),
                retryProperties = retryProps(),
                produceCircuitBreaker = closedProduceBreaker(),
                balanceMetrics = metrics(),
            )

        assertNotNull(errorHandler)
        assertTrue(errorHandler is DefaultErrorHandler)
    }

    @Test
    fun `should build retry topic names from max attempts`() {
        assertEquals(
            listOf(
                "transacoes-financeiras-processadas.retry-1",
                "transacoes-financeiras-processadas.retry-2",
                "transacoes-financeiras-processadas.retry-3",
            ),
            buildRetryTopicNames("transacoes-financeiras-processadas", 3),
        )
    }

    @Test
    fun `should publish technical failure to retry topic with attempt and not-before headers`() {
        val sent = mutableListOf<ProducerRecord<String, String>>()
        val kafkaOperations = recordingKafkaOperations(sent)
        val before = System.currentTimeMillis()

        val recoverer =
            createAsyncRetryRecoverer(
                kafkaOperations = kafkaOperations,
                retryTopics =
                    listOf(
                        "transacoes-financeiras-processadas.retry-1",
                        "transacoes-financeiras-processadas.retry-2",
                        "transacoes-financeiras-processadas.retry-3",
                    ),
                dltTopicName = "transacoes-financeiras-processadas.DLT",
                retryProperties = retryProps(),
                produceCircuitBreaker = closedProduceBreaker(),
                balanceMetrics = metrics(),
            )

        val record =
            ConsumerRecord(
                "transacoes-financeiras-processadas",
                1,
                10L,
                "account-1",
                """{"ok":true}""",
            )
        recoverer.accept(record, IllegalStateException("dynamo down"))

        assertEquals(1, sent.size)
        val outbound = sent.single()
        assertEquals("transacoes-financeiras-processadas.retry-1", outbound.topic())
        assertEquals(1, outbound.partition())
        assertEquals("account-1", outbound.key())
        assertEquals("1", String(outbound.headers().lastHeader(KafkaRetryHeaders.RETRY_ATTEMPT).value()))
        assertNotNull(outbound.headers().lastHeader(KafkaRetryHeaders.RETRY_FAILED_AT_MS))
        val notBefore =
            String(outbound.headers().lastHeader(KafkaRetryHeaders.RETRY_NOT_BEFORE_MS).value()).toLong()
        assertTrue(notBefore >= before)
        assertTrue(notBefore <= before + retryProps().maxIntervalMs + 5_000)
        assertEquals(
            "transacoes-financeiras-processadas",
            String(outbound.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value()),
        )
        assertNotNull(outbound.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN))
    }

    @Test
    fun `should publish not retryable failure to dlt without not-before header`() {
        val sent = mutableListOf<ProducerRecord<String, String>>()
        val kafkaOperations = recordingKafkaOperations(sent)

        val recoverer =
            createAsyncRetryRecoverer(
                kafkaOperations = kafkaOperations,
                retryTopics = listOf("t.retry-1", "t.retry-2", "t.retry-3"),
                dltTopicName = "t.DLT",
                retryProperties = retryProps(),
                produceCircuitBreaker = closedProduceBreaker(),
                balanceMetrics = metrics(),
            )

        val record = ConsumerRecord("t", 0, 1L, "k", "v")
        recoverer.accept(record, InvalidTransactionEventException("bad"))

        assertEquals(1, sent.size)
        assertEquals("t.DLT", sent.single().topic())
        assertNull(sent.single().headers().lastHeader(KafkaRetryHeaders.RETRY_NOT_BEFORE_MS))
    }

    @Test
    fun `should fail recoverer when kafka produce circuit is open`() {
        val sent = mutableListOf<ProducerRecord<String, String>>()
        val kafkaOperations = recordingKafkaOperations(sent)
        val recoverer =
            createAsyncRetryRecoverer(
                kafkaOperations = kafkaOperations,
                retryTopics = listOf("t.retry-1"),
                dltTopicName = "t.DLT",
                retryProperties = retryProps(),
                produceCircuitBreaker = openProduceBreaker(),
                balanceMetrics = metrics(),
            )

        assertFailsWith<DependencyUnavailableException> {
            recoverer.accept(ConsumerRecord("t", 0, 1L, "k", "v"), IllegalStateException("down"))
        }
        assertTrue(sent.isEmpty())
    }

    @Test
    fun `retry backoff stays within exponential bounds with full jitter`() {
        repeat(50) {
            val delay =
                RetryBackoff.delayMs(
                    attempt = 2,
                    initialIntervalMs = 1_000,
                    multiplier = 2.0,
                    maxIntervalMs = 30_000,
                )
            assertTrue(delay in 0L..2_000L)
        }
    }

    private fun closedProduceBreaker(): CircuitBreaker =
        CircuitBreakerRegistry.ofDefaults().circuitBreaker(CircuitBreakerNames.KAFKA_PRODUCE)

    private fun openProduceBreaker(): CircuitBreaker {
        val breaker = CircuitBreakerRegistry.ofDefaults().circuitBreaker(CircuitBreakerNames.KAFKA_PRODUCE)
        breaker.transitionToOpenState()
        return breaker
    }

    private fun recordingKafkaOperations(
        sent: MutableList<ProducerRecord<String, String>>,
    ): KafkaOperations<String, String> =
        object : KafkaOperations<String, String> by mock() {
            override fun send(record: ProducerRecord<String, String>): CompletableFuture<SendResult<String, String>> {
                sent.add(record)
                @Suppress("UNCHECKED_CAST")
                return CompletableFuture.completedFuture(mock(SendResult::class.java) as SendResult<String, String>)
            }
        }
}
