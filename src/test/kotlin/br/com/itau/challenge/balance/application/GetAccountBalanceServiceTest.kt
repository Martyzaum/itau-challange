package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.AccountBalanceNotFoundException
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class GetAccountBalanceServiceTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val accountBalance =
        AccountBalance(
            id = accountId,
            owner = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a"),
            balance = Balance(BigDecimal("183.12"), "BRL"),
            updatedAtMicros = 1_751_641_364_589_998,
            lastTransactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
        )

    @Test
    fun `should return account balance provided for account id`() {
        val service = GetAccountBalanceService(AccountBalanceProvider { accountBalance })

        val result = service.getAccountBalance(accountId)

        assertEquals(accountBalance, result)
    }

    @Test
    fun `should fail when account balance does not exist`() {
        val service = GetAccountBalanceService(AccountBalanceProvider { null })

        val exception =
            assertFailsWith<AccountBalanceNotFoundException> {
                service.getAccountBalance(accountId)
            }

        assertEquals("Account balance not found for account $accountId", exception.message)
    }
}
