package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import java.math.BigDecimal
import java.util.UUID

data class TransactionEvent(
    val transactionId: UUID,
    val transactionType: TransactionType,
    val transactionAmount: BigDecimal,
    val transactionCurrency: String,
    val transactionStatus: TransactionStatus,
    val timestampMicros: Long,
    val accountId: UUID,
    val accountOwner: UUID,
    val accountCreatedAtMicros: Long,
    val accountStatus: AccountStatus,
    val balance: Balance,
) {
    init {
        validateTransactionAmount()
        validateTimestamps()
        validateCurrency()
    }

    fun isEligibleForBalanceUpdate(): Boolean =
        transactionStatus.isBalanceEligible() && accountStatus.isBalanceEligible()

    private fun validateTransactionAmount() {
        if (transactionAmount <= BigDecimal.ZERO) {
            throw InvalidTransactionEventException("Transaction amount must be positive")
        }
    }

    private fun validateTimestamps() {
        if (timestampMicros <= 0) {
            throw InvalidTransactionEventException("Transaction timestamp must be positive")
        }
        if (accountCreatedAtMicros <= 0 || accountCreatedAtMicros > timestampMicros) {
            throw InvalidTransactionEventException(
                "Account creation timestamp must be positive and not after the transaction",
            )
        }
    }

    private fun validateCurrency() {
        if (transactionCurrency != balance.currency) {
            throw InvalidTransactionEventException("Transaction and balance currencies must match")
        }
    }
}
