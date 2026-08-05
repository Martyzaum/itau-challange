package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.AccountBalance

fun interface AccountBalanceRepository {
    fun saveIfNewer(accountBalance: AccountBalance): Boolean
}
