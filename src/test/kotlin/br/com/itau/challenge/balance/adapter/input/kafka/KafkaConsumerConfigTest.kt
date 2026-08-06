package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.DependencyUnavailableException
import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.config.CircuitBreakerNames
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
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
import kotlin.test.assertTrue

class KafkaConsumerConfigTest {

    @Test
    fun `should classify payload errors as not retryable and technical errors as retryable`() {
        val notRetryable = notRetryableExceptionTypes().toSet()

        assertTrue(JacksonException::class.java in notRetryable)
        assertTrue(IllegalArgumentException::class.java in notRetryable)
        assertTrue(NullPointerException::class.java in notRetryable)
        assertTrue(InvalidTransactionEventException::class.java in notRetryable)
        assertTrue(InvalidBalanceException::class.java in notRetryable)
        assertFalse(IllegalStateException::class.java in notRetryable)
        assertFalse(RuntimeException::class.java in notRetryable)
        assertTrue(isNotRetryable(InvalidTransactionEventException("bad")))
        assertTrue(isNotRetryable(NullPointerException("missing field")))
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
                produceCircuitBreaker = closedProduceBreaker(),
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
    fun `should build exponential retry delays capped by max interval`() {
        assertEquals(
            listOf(1000L, 5000L, 25000L),
            buildRetryDelaysMs(
                initialIntervalMs = 1000,
                multiplier = 5.0,
                maxIntervalMs = 30000,
                maxAttempts = 3,
            ),
        )
        assertEquals(
            listOf(1000L, 2000L, 4000L),
            buildRetryDelaysMs(
                initialIntervalMs = 1000,
                multiplier = 2.0,
                maxIntervalMs = 5000,
                maxAttempts = 3,
            ),
        )
    }

    @Test
    fun `should publish technical failure to retry topic with attempt headers`() {
        val sent = mutableListOf<ProducerRecord<String, String>>()
        val kafkaOperations = recordingKafkaOperations(sent)

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
                produceCircuitBreaker = closedProduceBreaker(),
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
        assertEquals(
            "transacoes-financeiras-processadas",
            String(outbound.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value()),
        )
        assertNotNull(outbound.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN))
    }

    @Test
    fun `should publish not retryable failure to dlt`() {
        val sent = mutableListOf<ProducerRecord<String, String>>()
        val kafkaOperations = recordingKafkaOperations(sent)

        val recoverer =
            createAsyncRetryRecoverer(
                kafkaOperations = kafkaOperations,
                retryTopics = listOf("t.retry-1", "t.retry-2", "t.retry-3"),
                dltTopicName = "t.DLT",
                produceCircuitBreaker = closedProduceBreaker(),
            )

        val record = ConsumerRecord("t", 0, 1L, "k", "v")
        recoverer.accept(record, InvalidTransactionEventException("bad"))

        assertEquals(1, sent.size)
        assertEquals("t.DLT", sent.single().topic())
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
                produceCircuitBreaker = openProduceBreaker(),
            )

        assertFailsWith<DependencyUnavailableException> {
            recoverer.accept(ConsumerRecord("t", 0, 1L, "k", "v"), IllegalStateException("down"))
        }
        assertTrue(sent.isEmpty())
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
