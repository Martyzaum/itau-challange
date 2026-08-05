package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidAccountBalanceException
import java.util.UUID

/**
 * Latest balance snapshot for an account.
 *
 * Version is the pair ([updatedAtMicros], [lastTransactionId]):
 * - higher timestamp wins
 * - equal timestamp: higher [lastTransactionId] string wins (deterministic tie-break)
 * - equal timestamp + equal transaction id: duplicate (at-least-once redelivery)
 */
data class AccountBalance(
    val id: UUID,
    val owner: UUID,
    val balance: Balance,
    val updatedAtMicros: Long,
    val lastTransactionId: UUID,
) {
    init {
        if (updatedAtMicros <= 0) {
            throw InvalidAccountBalanceException("Account balance update timestamp must be positive")
        }
    }

    fun isNewerThan(other: AccountBalance): Boolean {
        if (updatedAtMicros != other.updatedAtMicros) {
            return updatedAtMicros > other.updatedAtMicros
        }
        return lastTransactionId.toString() > other.lastTransactionId.toString()
    }
}
