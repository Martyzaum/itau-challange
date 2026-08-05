package br.com.itau.challenge.balance.adapter.input.kafka.dto

import java.math.BigDecimal

data class FinancialTransactionMessage(
    val transaction: TransactionPayload,
    val account: AccountPayload,
)

data class TransactionPayload(
    val id: String,
    val type: String,
    val amount: BigDecimal,
    val currency: String,
    val status: String,
    val timestamp: Long,
)

data class AccountPayload(
    val id: String,
    val owner: String,
    val created_at: Long,
    val status: String,
    val balance: BalancePayload,
)

data class BalancePayload(
    val amount: BigDecimal,
    val currency: String,
)
