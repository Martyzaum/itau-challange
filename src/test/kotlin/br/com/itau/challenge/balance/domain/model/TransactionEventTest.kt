package br.com.itau.challenge.balance.domain.model

import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class TransactionEventTest {

    private val transactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")
    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val balance = Balance(BigDecimal("183.12"), "BRL")

    private fun transactionEvent(
        transactionId: UUID = this.transactionId,
    ): TransactionEvent =
        TransactionEvent(
            transactionId = transactionId,
            transactionType = "CREDIT",
            transactionAmount = BigDecimal("97.07"),
            transactionCurrency = "BRL",
            transactionStatus = "APPROVED",
            timestampMicros = 1_751_641_364_589_998,
            accountId = accountId,
            accountOwner = ownerId,
            accountCreatedAtMicros = 1_634_874_339_000_000,
            accountStatus = "ENABLED",
            balance = balance,
        )

    @Test
    fun `should expose transaction and account data`() {
        val event = transactionEvent()

        assertEquals(transactionId, event.transactionId)
        assertEquals("CREDIT", event.transactionType)
        assertEquals(BigDecimal("97.07"), event.transactionAmount)
        assertEquals("BRL", event.transactionCurrency)
        assertEquals("APPROVED", event.transactionStatus)
        assertEquals(1_751_641_364_589_998, event.timestampMicros)
        assertEquals(accountId, event.accountId)
        assertEquals(ownerId, event.accountOwner)
        assertEquals(1_634_874_339_000_000, event.accountCreatedAtMicros)
        assertEquals("ENABLED", event.accountStatus)
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
}
