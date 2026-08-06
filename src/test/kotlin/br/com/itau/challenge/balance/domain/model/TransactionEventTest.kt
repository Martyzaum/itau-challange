package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class TransactionEventTest {

    private val transactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")
    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val balance = Balance(BigDecimal("183.12"), "BRL")

    @Test
    fun `should expose transaction and account data`() {
        val event = transactionEvent()

        assertEquals(transactionId, event.transactionId)
        assertEquals(TransactionType.CREDIT, event.transactionType)
        assertEquals(BigDecimal("97.07"), event.transactionAmount)
        assertEquals("BRL", event.transactionCurrency)
        assertEquals(TransactionStatus.APPROVED, event.transactionStatus)
        assertEquals(1_751_641_364_589_998, event.timestampMicros)
        assertEquals(accountId, event.accountId)
        assertEquals(ownerId, event.accountOwner)
        assertEquals(1_634_874_339_000_000, event.accountCreatedAtMicros)
        assertEquals(AccountStatus.ENABLED, event.accountStatus)
        assertEquals(balance, event.balance)
    }

    @Test
    fun `should be equal when all event data is equal`() {
        assertEquals(transactionEvent(), transactionEvent())
    }

    @Test
    fun `should not be equal when transaction id differs`() {
        assertNotEquals(
            transactionEvent(),
            transactionEvent(transactionId = UUID.fromString("acaf7d4c-8c00-46ec-a401-877e843feabb")),
        )
    }

    @Test
    fun `should accept credit and debit transaction types`() {
        transactionEvent(transactionType = "CREDIT")
        transactionEvent(transactionType = "DEBIT")
    }

    @Test
    fun `should reject unknown transaction type`() {
        assertInvalidEvent("Transaction type must be CREDIT or DEBIT") {
            transactionEvent(transactionType = "TRANSFER")
        }
    }

    @Test
    fun `should accept approved declined and rejected transaction statuses`() {
        transactionEvent(transactionStatus = "APPROVED")
        transactionEvent(transactionStatus = "DECLINED")
        transactionEvent(transactionStatus = "REJECTED")
        assertFalse(transactionEvent(transactionStatus = "REJECTED").isEligibleForBalanceUpdate())
    }

    @Test
    fun `should reject unknown transaction status`() {
        assertInvalidEvent("Transaction status must be APPROVED, DECLINED or REJECTED") {
            transactionEvent(transactionStatus = "PENDING")
        }
    }

    @Test
    fun `should reject non-positive transaction amount`() {
        assertInvalidEvent("Transaction amount must be positive") {
            transactionEvent(transactionAmount = BigDecimal.ZERO)
        }
    }

    @Test
    fun `should reject non-positive transaction timestamp`() {
        assertInvalidEvent("Transaction timestamp must be positive") {
            transactionEvent(timestampMicros = 0)
        }
    }

    @Test
    fun `should reject non-positive account creation timestamp`() {
        assertInvalidEvent("Account creation timestamp must be positive and not after the transaction") {
            transactionEvent(accountCreatedAtMicros = 0)
        }
    }

    @Test
    fun `should reject account created after transaction`() {
        assertInvalidEvent("Account creation timestamp must be positive and not after the transaction") {
            transactionEvent(accountCreatedAtMicros = 1_751_641_364_590_000)
        }
    }

    @Test
    fun `should accept enabled and disabled account statuses`() {
        transactionEvent(accountStatus = "ENABLED")
        transactionEvent(accountStatus = "DISABLED")
    }

    @Test
    fun `should reject unknown account status`() {
        assertInvalidEvent("Account status must be ENABLED or DISABLED") {
            transactionEvent(accountStatus = "BLOCKED")
        }
    }

    @Test
    fun `should reject transaction currency different from balance currency`() {
        assertInvalidEvent("Transaction and balance currencies must match") {
            transactionEvent(transactionCurrency = "USD")
        }
    }

    @Test
    fun `should be eligible when transaction is approved and account is enabled`() {
        assertTrue(transactionEvent().isEligibleForBalanceUpdate())
    }

    @Test
    fun `should not be eligible when transaction is declined`() {
        assertFalse(transactionEvent(transactionStatus = "DECLINED").isEligibleForBalanceUpdate())
    }

    @Test
    fun `should not be eligible when account is disabled`() {
        assertFalse(transactionEvent(accountStatus = "DISABLED").isEligibleForBalanceUpdate())
    }

    private fun transactionEvent(
        transactionId: UUID = this.transactionId,
        transactionType: String = "CREDIT",
        transactionAmount: BigDecimal = BigDecimal("97.07"),
        transactionCurrency: String = "BRL",
        transactionStatus: String = "APPROVED",
        timestampMicros: Long = 1_751_641_364_589_998,
        accountCreatedAtMicros: Long = 1_634_874_339_000_000,
        accountStatus: String = "ENABLED",
    ): TransactionEvent =
        TransactionEvent(
            transactionId = transactionId,
            transactionType = TransactionType.parse(transactionType),
            transactionAmount = transactionAmount,
            transactionCurrency = transactionCurrency,
            transactionStatus = TransactionStatus.parse(transactionStatus),
            timestampMicros = timestampMicros,
            accountId = accountId,
            accountOwner = ownerId,
            accountCreatedAtMicros = accountCreatedAtMicros,
            accountStatus = AccountStatus.parse(accountStatus),
            balance = balance,
        )

    private fun assertInvalidEvent(
        expectedMessage: String,
        block: () -> Unit,
    ) {
        val exception = assertFailsWith<InvalidTransactionEventException>(block = block)

        assertEquals(expectedMessage, exception.message)
    }
}
