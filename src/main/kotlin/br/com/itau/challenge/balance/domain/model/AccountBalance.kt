package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidAccountBalanceException
import java.util.UUID

data class AccountBalance(
    val id: UUID,
    val owner: UUID,
    val balance: Balance,
    val updatedAtMicros: Long,
) {
    init {
        if (updatedAtMicros <= 0) {
            throw InvalidAccountBalanceException("Account balance update timestamp must be positive")
        }
    }
}
