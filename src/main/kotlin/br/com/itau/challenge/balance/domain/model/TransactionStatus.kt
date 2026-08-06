package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException

enum class TransactionStatus {
    APPROVED,
    DECLINED,
    REJECTED,
    ;

    fun isBalanceEligible(): Boolean = this == APPROVED

    companion object {
        fun parse(raw: String): TransactionStatus =
            entries.find { it.name == raw }
                ?: throw InvalidTransactionEventException(
                    "Transaction status must be APPROVED, DECLINED or REJECTED",
                )
    }
}
