package br.com.itau.challenge.balance.application

import br.com.itau.challenge.balance.domain.exception.AccountBalanceNotFoundException
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class GetAccountBalanceService(
    private val accountBalanceProvider: AccountBalanceProvider,
) : GetAccountBalanceUseCase {

    override fun getAccountBalance(accountId: UUID): AccountBalance =
        accountBalanceProvider.findByAccountId(accountId)
            ?: throw AccountBalanceNotFoundException(accountId)
}
