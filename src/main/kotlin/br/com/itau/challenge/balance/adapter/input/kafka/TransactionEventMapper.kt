package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import java.util.UUID

internal fun FinancialTransactionMessage.toDomain(): TransactionEvent =
    TransactionEvent(
        transactionId = UUID.fromString(transaction.id),
        transactionType = transaction.type,
        transactionAmount = transaction.amount,
        transactionCurrency = transaction.currency,
        transactionStatus = transaction.status,
        timestampMicros = transaction.timestamp,
        accountId = UUID.fromString(account.id),
        accountOwner = UUID.fromString(account.owner),
        accountCreatedAtMicros = account.created_at,
        accountStatus = account.status,
        balance =
            Balance(
                amount = account.balance.amount,
                currency = account.balance.currency,
            ),
    )
