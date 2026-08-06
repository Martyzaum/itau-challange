package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.domain.model.AccountStatus
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.domain.model.TransactionEvent
import br.com.itau.challenge.balance.domain.model.TransactionStatus
import br.com.itau.challenge.balance.domain.model.TransactionType
import java.util.UUID

internal fun FinancialTransactionMessage.toDomain(): TransactionEvent =
    TransactionEvent(
        transactionId = UUID.fromString(transaction.id),
        transactionType = TransactionType.parse(transaction.type),
        transactionAmount = transaction.amount,
        transactionCurrency = transaction.currency,
        transactionStatus = TransactionStatus.parse(transaction.status),
        timestampMicros = transaction.timestamp,
        accountId = UUID.fromString(account.id),
        accountOwner = UUID.fromString(account.owner),
        accountCreatedAtMicros = account.created_at,
        accountStatus = AccountStatus.parse(account.status),
        balance =
            Balance(
                amount = account.balance.amount,
                currency = account.balance.currency,
            ),
    )
