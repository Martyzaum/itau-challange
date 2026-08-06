package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.ProcessTransactionResult
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository

class ProcessTransactionEventService(
    private val accountBalanceRepository: AccountBalanceRepository,
) : ProcessTransactionEventUseCase {

    override fun processTransactionEvent(event: TransactionEvent): ProcessTransactionResult {
        if (!event.isEligibleForBalanceUpdate()) {
            return ProcessTransactionResult.IgnoredIneligible
        }

        val saved =
            accountBalanceRepository.saveIfNewer(
                AccountBalance(
                    id = event.accountId,
                    owner = event.accountOwner,
                    balance = event.balance,
                    updatedAtMicros = event.timestampMicros,
                    lastTransactionId = event.transactionId,
                ),
            )
        return if (saved) {
            ProcessTransactionResult.Saved
        } else {
            ProcessTransactionResult.IgnoredNotNewer
        }
    }
}
