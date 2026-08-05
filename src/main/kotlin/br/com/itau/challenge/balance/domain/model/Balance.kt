package br.com.itau.challenge.balance.domain.model

import java.math.BigDecimal

data class Balance(
    val amount: BigDecimal,
    val currency: String,
)
