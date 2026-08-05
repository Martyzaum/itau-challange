package br.com.itau.challenge.balance.domain.model

import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class BalanceTest {

    @Test
    fun `should expose amount and currency`() {
        val balance = Balance(amount = BigDecimal("183.12"), currency = "BRL")

        assertEquals(BigDecimal("183.12"), balance.amount)
        assertEquals("BRL", balance.currency)
    }

    @Test
    fun `should be equal when amount and currency are equal`() {
        assertEquals(
            Balance(BigDecimal("183.12"), "BRL"),
            Balance(BigDecimal("183.12"), "BRL"),
        )
    }

    @Test
    fun `should not be equal when amount differs`() {
        assertNotEquals(
            Balance(BigDecimal("183.12"), "BRL"),
            Balance(BigDecimal("184.12"), "BRL"),
        )
    }
}
