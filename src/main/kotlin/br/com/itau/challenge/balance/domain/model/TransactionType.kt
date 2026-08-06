package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException

enum class TransactionType {
    CREDIT,
    DEBIT,
    ;

    companion object {
        fun parse(raw: String): TransactionType =
            entries.find { it.name == raw }
                ?: throw InvalidTransactionEventException("Transaction type must be CREDIT or DEBIT")
    }
}
