package br.com.itau.challenge.balance.domain.model

import java.util.UUID

data class AccountBalance(
    val id: UUID,
    val owner: UUID,
    val balance: Balance,
    val updatedAtMicros: Long,
)
