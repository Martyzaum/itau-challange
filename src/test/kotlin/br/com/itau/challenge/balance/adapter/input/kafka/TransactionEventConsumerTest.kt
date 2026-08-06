package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper
import java.nio.charset.StandardCharsets
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TransactionEventConsumerTest {

    private val objectMapper = JsonMapper.builder().build()
    private val meterRegistry = SimpleMeterRegistry()
    private val balanceMetrics = BalanceMetrics(meterRegistry)

    @Test
    fun `should deserialize valid payload delegate to use case and count saved`() {
        val processed = mutableListOf<TransactionEvent>()
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase =
                    ProcessTransactionEventUseCase {
                        processed.add(it)
                        true
                    },
                objectMapper = objectMapper,
                balanceMetrics = balanceMetrics,
                retryDelaysCsv = "1000,5000,30000",
            )

        consumer.consume(validPayload())

        assertEquals(1, processed.size)
        assertEquals(UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"), processed.single().accountId)
        assertEquals(1.0, meterRegistry.counter("balance.transactions", "result", "saved").count())
    }

    @Test
    fun `should count ignored transactions`() {
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase = ProcessTransactionEventUseCase { false },
                objectMapper = objectMapper,
                balanceMetrics = balanceMetrics,
                retryDelaysCsv = "1000,5000,30000",
            )

        consumer.consume(validPayload())

        assertEquals(1.0, meterRegistry.counter("balance.transactions", "result", "ignored").count())
    }

    @Test
    fun `should fail on invalid json so error handler can route to dlt`() {
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase = ProcessTransactionEventUseCase { true },
                objectMapper = objectMapper,
                balanceMetrics = balanceMetrics,
                retryDelaysCsv = "1000,5000,30000",
            )

        assertFailsWith<JacksonException> {
            consumer.consume("{not-json")
        }
    }

    @Test
    fun `should fail on invalid domain payload so error handler can route to dlt`() {
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase = ProcessTransactionEventUseCase { true },
                objectMapper = objectMapper,
                balanceMetrics = balanceMetrics,
                retryDelaysCsv = "1000,5000,30000",
            )

        assertFailsWith<InvalidTransactionEventException> {
            consumer.consume(validPayload(type = "TRANSFER"))
        }
    }

    @Test
    fun `should propagate unexpected processing failures for retry`() {
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase =
                    ProcessTransactionEventUseCase {
                        throw IllegalStateException("dynamodb unavailable")
                    },
                objectMapper = objectMapper,
                balanceMetrics = balanceMetrics,
                retryDelaysCsv = "1000,5000,30000",
            )

        val exception =
            assertFailsWith<IllegalStateException> {
                consumer.consume(validPayload())
            }

        assertTrue(exception.message!!.contains("dynamodb unavailable"))
    }

    @Test
    fun `should process retry record after delay header elapsed`() {
        val processed = mutableListOf<TransactionEvent>()
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase =
                    ProcessTransactionEventUseCase {
                        processed.add(it)
                        true
                    },
                objectMapper = objectMapper,
                balanceMetrics = balanceMetrics,
                retryDelaysCsv = "1,1,1",
            )
        val record =
            ConsumerRecord(
                "transacoes-financeiras-processadas.retry-1",
                0,
                1L,
                "account",
                validPayload(),
            )
        record.headers().add(
            RecordHeader(KafkaRetryHeaders.RETRY_ATTEMPT, "1".toByteArray(StandardCharsets.UTF_8)),
        )
        record.headers().add(
            RecordHeader(
                KafkaRetryHeaders.RETRY_FAILED_AT_MS,
                (System.currentTimeMillis() - 100).toString().toByteArray(StandardCharsets.UTF_8),
            ),
        )

        consumer.consumeRetry(record)

        assertEquals(1, processed.size)
    }

    private fun validPayload(
        type: String = "CREDIT",
        status: String = "APPROVED",
    ): String =
        """
        {
          "transaction": {
            "id": "8e8ae808-b154-48b5-9f3e-553935cc4543",
            "type": "$type",
            "amount": 97.07,
            "currency": "BRL",
            "status": "$status",
            "timestamp": 1751641364589998
          },
          "account": {
            "id": "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
            "owner": "315e3cfe-f4af-4cd2-b298-a449e614349a",
            "created_at": 1634874339000000,
            "status": "ENABLED",
            "balance": {
              "amount": 183.12,
              "currency": "BRL"
            }
          }
        }
        """.trimIndent()
}
