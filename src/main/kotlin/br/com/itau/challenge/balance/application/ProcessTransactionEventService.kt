package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.springframework.stereotype.Service

@Service
class ProcessTransactionEventService(
    private val accountBalanceRepository: AccountBalanceRepository,
) : ProcessTransactionEventUseCase {

    override fun processTransactionEvent(event: TransactionEvent): Boolean {
        if (!event.isEligibleForBalanceUpdate()) {
            return false
        }

        return accountBalanceRepository.saveIfNewer(
            AccountBalance(
                id = event.accountId,
                owner = event.accountOwner,
                balance = event.balance,
                updatedAtMicros = event.timestampMicros,
                lastTransactionId = event.transactionId,
            ),
        )
    }
}
