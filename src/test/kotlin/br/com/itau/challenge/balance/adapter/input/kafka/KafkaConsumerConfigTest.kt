package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DefaultErrorHandler
import tools.jackson.core.JacksonException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class KafkaConsumerConfigTest {

    @Test
    fun `should configure exponential backoff attempts and intervals`() {
        val backOff =
            createRetryBackOff(
                initialIntervalMs = 500,
                multiplier = 2.0,
                maxIntervalMs = 5000,
                maxAttempts = 3,
            )

        assertEquals(500, backOff.initialInterval)
        assertEquals(2.0, backOff.multiplier)
        assertEquals(5000, backOff.maxInterval)
        assertEquals(3, backOff.maxRetries)
    }

    @Test
    fun `should classify payload errors as not retryable and technical errors as retryable`() {
        val notRetryable = notRetryableExceptionTypes().toSet()

        assertTrue(JacksonException::class.java in notRetryable)
        assertTrue(IllegalArgumentException::class.java in notRetryable)
        assertTrue(InvalidTransactionEventException::class.java in notRetryable)
        assertTrue(InvalidBalanceException::class.java in notRetryable)
        assertFalse(IllegalStateException::class.java in notRetryable)
        assertFalse(RuntimeException::class.java in notRetryable)
    }

    @Test
    fun `should route dlt records to configured topic keeping original partition`() {
        val record =
            ConsumerRecord(
                "transacoes-financeiras-processadas",
                2,
                15L,
                "account-1",
                """{"bad":true}""",
            )

        val destination =
            dltTopicPartition(
                dltTopicName = "transacoes-financeiras-processadas.DLT",
                record = record,
            )

        assertEquals("transacoes-financeiras-processadas.DLT", destination.topic())
        assertEquals(2, destination.partition())
    }

    @Test
    fun `should build default error handler bean wiring`() {
        @Suppress("UNCHECKED_CAST")
        val kafkaOperations = mock(KafkaOperations::class.java) as KafkaOperations<String, String>

        val errorHandler =
            createKafkaErrorHandler(
                kafkaOperations = kafkaOperations,
                dltTopicName = "transacoes-financeiras-processadas.DLT",
                backOff = createRetryBackOff(500, 2.0, 5000, 3),
            )

        assertNotNull(errorHandler)
        assertTrue(errorHandler is DefaultErrorHandler)
    }
}
