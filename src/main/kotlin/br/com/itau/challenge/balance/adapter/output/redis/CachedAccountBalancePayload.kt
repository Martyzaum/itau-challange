package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import java.math.BigDecimal
import java.util.UUID

data class CachedAccountBalancePayload(
    val id: String,
    val owner: String,
    val amount: String,
    val currency: String,
    val updatedAtMicros: Long,
    val lastTransactionId: String,
) {
    fun toDomain(): AccountBalance =
        AccountBalance(
            id = UUID.fromString(id),
            owner = UUID.fromString(owner),
            balance = Balance(amount = BigDecimal(amount), currency = currency),
            updatedAtMicros = updatedAtMicros,
            lastTransactionId = UUID.fromString(lastTransactionId),
        )

    companion object {
        fun from(balance: AccountBalance): CachedAccountBalancePayload =
            CachedAccountBalancePayload(
                id = balance.id.toString(),
                owner = balance.owner.toString(),
                amount = balance.balance.amount.toPlainString(),
                currency = balance.balance.currency,
                updatedAtMicros = balance.updatedAtMicros,
                lastTransactionId = balance.lastTransactionId.toString(),
            )
    }
}
