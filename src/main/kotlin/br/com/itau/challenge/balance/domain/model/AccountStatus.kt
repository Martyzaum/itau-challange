package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException

enum class AccountStatus {
    ENABLED,
    DISABLED,
    ;

    fun isBalanceEligible(): Boolean = this == ENABLED

    companion object {
        fun parse(raw: String): AccountStatus =
            entries.find { it.name == raw }
                ?: throw InvalidTransactionEventException("Account status must be ENABLED or DISABLED")
    }
}
