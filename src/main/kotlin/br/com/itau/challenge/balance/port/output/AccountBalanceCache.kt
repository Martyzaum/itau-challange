package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.AccountBalance
import java.util.UUID

interface AccountBalanceCache {
    fun get(accountId: UUID): AccountBalance?

    fun putIfNewer(balance: AccountBalance): Boolean

    fun invalidate(accountId: UUID)
}
