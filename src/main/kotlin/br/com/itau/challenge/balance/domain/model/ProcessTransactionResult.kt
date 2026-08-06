package br.com.itau.challenge.balance.domain.model

sealed interface ProcessTransactionResult {
    data object Saved : ProcessTransactionResult

    data object IgnoredIneligible : ProcessTransactionResult

    data object IgnoredNotNewer : ProcessTransactionResult
}
