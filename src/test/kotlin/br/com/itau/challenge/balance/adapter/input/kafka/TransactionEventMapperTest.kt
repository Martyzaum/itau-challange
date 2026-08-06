package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.AccountPayload
import br.com.itau.challenge.balance.adapter.input.kafka.dto.BalancePayload
import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.adapter.input.kafka.dto.TransactionPayload
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.balance.domain.model.Balance
import tools.jackson.databind.json.JsonMapper
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TransactionEventMapperTest {

    private val objectMapper = JsonMapper.builder().build()

    @Test
    fun `should map valid payload to domain event`() {
        val message =
            FinancialTransactionMessage(
                transaction =
                    TransactionPayload(
                        id = "8e8ae808-b154-48b5-9f3e-553935cc4543",
                        type = "CREDIT",
                        amount = BigDecimal("97.07"),
                        currency = "BRL",
                        status = "APPROVED",
                        timestamp = 1_751_641_364_589_998,
                    ),
                account =
                    AccountPayload(
                        id = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
                        owner = "315e3cfe-f4af-4cd2-b298-a449e614349a",
                        created_at = 1_634_874_339_000_000,
                        status = "ENABLED",
                        balance = BalancePayload(amount = BigDecimal("183.12"), currency = "BRL"),
                    ),
            )

        val event = message.toDomain()

        assertEquals(UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"), event.transactionId)
        assertEquals(br.com.itau.challenge.balance.domain.model.TransactionType.CREDIT, event.transactionType)
        assertEquals(BigDecimal("97.07"), event.transactionAmount)
        assertEquals("BRL", event.transactionCurrency)
        assertEquals(br.com.itau.challenge.balance.domain.model.TransactionStatus.APPROVED, event.transactionStatus)
        assertEquals(1_751_641_364_589_998, event.timestampMicros)
        assertEquals(UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975"), event.accountId)
        assertEquals(UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a"), event.accountOwner)
        assertEquals(1_634_874_339_000_000, event.accountCreatedAtMicros)
        assertEquals(br.com.itau.challenge.balance.domain.model.AccountStatus.ENABLED, event.accountStatus)
        assertEquals(Balance(BigDecimal("183.12"), "BRL"), event.balance)
    }

    @Test
    fun `should deserialize challenge payload json`() {
        val json =
            """
            {
              "transaction": {
                "id": "8e8ae808-b154-48b5-9f3e-553935cc4543",
                "type": "CREDIT",
                "amount": 97.07,
                "currency": "BRL",
                "status": "APPROVED",
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

        val message = objectMapper.readValue(json, FinancialTransactionMessage::class.java)
        val event = message.toDomain()

        assertEquals(BigDecimal("97.07"), event.transactionAmount)
        assertEquals(BigDecimal("183.12"), event.balance.amount)
        assertEquals(1_751_641_364_589_998, event.timestampMicros)
    }

    @Test
    fun `should fail when transaction id is not a uuid`() {
        val message =
            FinancialTransactionMessage(
                transaction =
                    TransactionPayload(
                        id = "not-a-uuid",
                        type = "CREDIT",
                        amount = BigDecimal("97.07"),
                        currency = "BRL",
                        status = "APPROVED",
                        timestamp = 1_751_641_364_589_998,
                    ),
                account =
                    AccountPayload(
                        id = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
                        owner = "315e3cfe-f4af-4cd2-b298-a449e614349a",
                        created_at = 1_634_874_339_000_000,
                        status = "ENABLED",
                        balance = BalancePayload(amount = BigDecimal("183.12"), currency = "BRL"),
                    ),
            )

        assertFailsWith<InvalidTransactionEventException> {
            message.toDomain()
        }
    }

    @Test
    fun `should fail domain validation for unknown transaction type`() {
        val message =
            FinancialTransactionMessage(
                transaction =
                    TransactionPayload(
                        id = "8e8ae808-b154-48b5-9f3e-553935cc4543",
                        type = "TRANSFER",
                        amount = BigDecimal("97.07"),
                        currency = "BRL",
                        status = "APPROVED",
                        timestamp = 1_751_641_364_589_998,
                    ),
                account =
                    AccountPayload(
                        id = "5b19c8b6-0cc4-4c72-a989-0c2ee15fa975",
                        owner = "315e3cfe-f4af-4cd2-b298-a449e614349a",
                        created_at = 1_634_874_339_000_000,
                        status = "ENABLED",
                        balance = BalancePayload(amount = BigDecimal("183.12"), currency = "BRL"),
                    ),
            )

        assertFailsWith<InvalidTransactionEventException> {
            message.toDomain()
        }
    }
}
