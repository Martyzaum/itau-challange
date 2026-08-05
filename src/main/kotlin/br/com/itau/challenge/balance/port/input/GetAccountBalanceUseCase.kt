package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.AccountBalance
import java.util.UUID

fun interface GetAccountBalanceUseCase {
    fun getAccountBalance(accountId: UUID): AccountBalance
}
