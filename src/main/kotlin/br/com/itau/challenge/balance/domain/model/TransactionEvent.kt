package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import java.math.BigDecimal
import java.util.UUID

private const val APPROVED_STATUS = "APPROVED"
private const val DECLINED_STATUS = "DECLINED"
private const val REJECTED_STATUS = "REJECTED"
private const val ENABLED_STATUS = "ENABLED"
private val TRANSACTION_TYPES = setOf("CREDIT", "DEBIT")
private val TRANSACTION_STATUSES = setOf(APPROVED_STATUS, DECLINED_STATUS, REJECTED_STATUS)
private val ACCOUNT_STATUSES = setOf(ENABLED_STATUS, "DISABLED")

data class TransactionEvent(
    val transactionId: UUID,
    val transactionType: String,
    val transactionAmount: BigDecimal,
    val transactionCurrency: String,
    val transactionStatus: String,
    val timestampMicros: Long,
    val accountId: UUID,
    val accountOwner: UUID,
    val accountCreatedAtMicros: Long,
    val accountStatus: String,
    val balance: Balance,
) {
    init {
        validateTransactionType()
        validateTransactionStatus()
        validateTransactionAmount()
        validateTimestamps()
        validateAccountStatus()
        validateCurrency()
    }

    fun isEligibleForBalanceUpdate(): Boolean =
        transactionStatus == APPROVED_STATUS && accountStatus == ENABLED_STATUS

    private fun validateTransactionType() {
        if (transactionType !in TRANSACTION_TYPES) {
            throw InvalidTransactionEventException("Transaction type must be CREDIT or DEBIT")
        }
    }

    private fun validateTransactionStatus() {
        if (transactionStatus !in TRANSACTION_STATUSES) {
            throw InvalidTransactionEventException(
                "Transaction status must be APPROVED, DECLINED or REJECTED",
            )
        }
    }

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
            throw InvalidTransactionEventException("Account creation timestamp must be positive and not after the transaction")
        }
    }

    private fun validateAccountStatus() {
        if (accountStatus !in ACCOUNT_STATUSES) {
            throw InvalidTransactionEventException("Account status must be ENABLED or DISABLED")
        }
    }

    private fun validateCurrency() {
        if (transactionCurrency != balance.currency) {
            throw InvalidTransactionEventException("Transaction and balance currencies must match")
        }
    }
}
