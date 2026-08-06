package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.domain.model.ProcessTransactionResult
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProcessTransactionEventServiceTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val balance = Balance(BigDecimal("183.12"), "BRL")

    @Test
    fun `should save eligible event as account balance snapshot`() {
        var savedBalance: AccountBalance? = null
        val repository =
            AccountBalanceRepository {
                savedBalance = it
                true
            }
        val service = ProcessTransactionEventService(repository)

        val result = service.processTransactionEvent(transactionEvent())

        assertEquals(ProcessTransactionResult.Saved, result)
        assertEquals(
            AccountBalance(
                id = accountId,
                owner = ownerId,
                balance = balance,
                updatedAtMicros = 1_751_641_364_589_998,
                lastTransactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
            ),
            savedBalance,
        )
    }

    @Test
    fun `should return ignored not newer when repository rejects snapshot`() {
        val service = ProcessTransactionEventService(AccountBalanceRepository { false })

        val result = service.processTransactionEvent(transactionEvent())

        assertEquals(ProcessTransactionResult.IgnoredNotNewer, result)
    }

    @Test
    fun `should ignore declined transaction without calling repository`() {
        var savedBalance: AccountBalance? = null
        val service =
            ProcessTransactionEventService(
                AccountBalanceRepository {
                    savedBalance = it
                    true
                },
            )

        val result = service.processTransactionEvent(transactionEvent(transactionStatus = "DECLINED"))

        assertEquals(ProcessTransactionResult.IgnoredIneligible, result)
        assertNull(savedBalance)
    }

    @Test
    fun `should ignore disabled account without calling repository`() {
        var savedBalance: AccountBalance? = null
        val service =
            ProcessTransactionEventService(
                AccountBalanceRepository {
                    savedBalance = it
                    true
                },
            )

        val result = service.processTransactionEvent(transactionEvent(accountStatus = "DISABLED"))

        assertEquals(ProcessTransactionResult.IgnoredIneligible, result)
        assertNull(savedBalance)
    }

    private fun transactionEvent(
        transactionStatus: String = "APPROVED",
        accountStatus: String = "ENABLED",
    ): TransactionEvent =
        TransactionEvent(
            transactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
            transactionType = "CREDIT",
            transactionAmount = BigDecimal("97.07"),
            transactionCurrency = "BRL",
            transactionStatus = transactionStatus,
            timestampMicros = 1_751_641_364_589_998,
            accountId = accountId,
            accountOwner = ownerId,
            accountCreatedAtMicros = 1_634_874_339_000_000,
            accountStatus = accountStatus,
            balance = balance,
        )
}
