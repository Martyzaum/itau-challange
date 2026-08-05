package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidAccountBalanceException
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class AccountBalanceTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val balance = Balance(BigDecimal("183.12"), "BRL")

    @Test
    fun `should expose account balance data`() {
        val accountBalance = accountBalance()

        assertEquals(accountId, accountBalance.id)
        assertEquals(ownerId, accountBalance.owner)
        assertEquals(balance, accountBalance.balance)
        assertEquals(1_751_641_364_589_998, accountBalance.updatedAtMicros)
    }

    @Test
    fun `should be equal when all account balance data is equal`() {
        assertEquals(accountBalance(), accountBalance())
    }

    @Test
    fun `should not be equal when update timestamp differs`() {
        assertNotEquals(
            accountBalance(),
            accountBalance(updatedAtMicros = 1_751_641_364_590_000),
        )
    }

    @Test
    fun `should reject non-positive update timestamp`() {
        val exception =
            assertFailsWith<InvalidAccountBalanceException> {
                accountBalance(updatedAtMicros = 0)
            }

        assertEquals("Account balance update timestamp must be positive", exception.message)
    }

    private fun accountBalance(
        updatedAtMicros: Long = 1_751_641_364_589_998,
    ): AccountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = balance,
            updatedAtMicros = updatedAtMicros,
        )
}
