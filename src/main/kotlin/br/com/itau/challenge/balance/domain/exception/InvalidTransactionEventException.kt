package br.com.itau.challenge.balance.domain.exception

class InvalidTransactionEventException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)
