package br.com.itau.challenge.balance.port.input

import br.com.itau.challenge.balance.domain.model.TransactionEvent

fun interface ProcessTransactionEventUseCase {
    fun processTransactionEvent(event: TransactionEvent): Boolean
}
