package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import java.math.BigDecimal
import java.util.Currency

private const val ISO_CURRENCY_CODE_LENGTH = 3

data class Balance(
    val amount: BigDecimal,
    val currency: String,
) {
    init {
        if (!currency.isIso4217Currency()) {
            throw InvalidBalanceException("Balance currency must be a valid uppercase ISO 4217 code")
        }
    }

    private fun String.isIso4217Currency(): Boolean =
        length == ISO_CURRENCY_CODE_LENGTH &&
            this == uppercase() &&
            runCatching { Currency.getInstance(this) }.isSuccess
}
