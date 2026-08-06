package br.com.itau.challenge.balance.domain.model

import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.Currency

private const val ISO_CURRENCY_CODE_LENGTH = 3

data class Balance private constructor(
    val amount: BigDecimal,
    val currency: String,
) {
    fun hasSameMoneyAs(other: Balance): Boolean =
        currency == other.currency && amount.compareTo(other.amount) == 0

    companion object {
        operator fun invoke(
            amount: BigDecimal,
            currency: String,
        ): Balance = of(amount, currency)

        fun of(
            amount: BigDecimal,
            currency: String,
        ): Balance {
            if (!currency.isValidIso4217()) {
                throw InvalidBalanceException("Balance currency must be a valid uppercase ISO 4217 code")
            }
            val scale = Currency.getInstance(currency).defaultFractionDigits.coerceAtLeast(0)
            return Balance(
                amount = amount.setScale(scale, RoundingMode.HALF_EVEN),
                currency = currency,
            )
        }

        private fun String.isValidIso4217(): Boolean =
            length == ISO_CURRENCY_CODE_LENGTH &&
                this == uppercase() &&
                runCatching { Currency.getInstance(this) }.isSuccess
    }
}
