package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import tools.jackson.core.JacksonException
import tools.jackson.databind.json.JsonMapper
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TransactionEventConsumerTest {

    private val objectMapper = JsonMapper.builder().build()

    @Test
    fun `should deserialize valid payload and delegate to use case`() {
        val processed = mutableListOf<TransactionEvent>()
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase =
                    ProcessTransactionEventUseCase {
                        processed.add(it)
                        true
                    },
                objectMapper = objectMapper,
            )

        consumer.consume(validPayload())

        assertEquals(1, processed.size)
        assertEquals(UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"), processed.single().accountId)
        assertEquals("APPROVED", processed.single().transactionStatus)
    }

    @Test
    fun `should fail on invalid json so error handler can route to dlt`() {
        val consumer =
            TransactionEventConsumer(
                processTransactionEventUseCase = ProcessTransactionEventUseCase { true },
                objectMapper = objectMapper,
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
            )

        val exception =
            assertFailsWith<IllegalStateException> {
                consumer.consume(validPayload())
            }

        assertTrue(exception.message!!.contains("dynamodb unavailable"))
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
