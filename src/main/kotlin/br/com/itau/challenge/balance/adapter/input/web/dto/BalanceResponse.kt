package br.com.itau.challenge.balance.adapter.input.web.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.math.BigDecimal
import java.util.UUID

data class BalanceResponse(
    val id: UUID,
    val owner: UUID,
    val balance: MoneyResponse,
    @get:JsonProperty("updated_at")
    val updatedAt: String,
)

data class MoneyResponse(
    val amount: BigDecimal,
    val currency: String,
)
