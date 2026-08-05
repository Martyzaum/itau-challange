package br.com.itau.challenge.balance.domain.model

import java.math.BigDecimal
import java.util.UUID

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
)
