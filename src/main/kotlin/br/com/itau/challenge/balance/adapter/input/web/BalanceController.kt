package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.web.dto.BalanceResponse
import br.com.itau.challenge.balance.adapter.input.web.dto.MoneyResponse
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
class BalanceController(
    private val getAccountBalanceUseCase: GetAccountBalanceUseCase,
) {

    @GetMapping("/balances/{accountId}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getBalance(@PathVariable accountId: UUID): BalanceResponse =
        getAccountBalanceUseCase.getAccountBalance(accountId).toResponse()
}

private fun AccountBalance.toResponse(): BalanceResponse =
    BalanceResponse(
        id = id,
        owner = owner,
        balance =
            MoneyResponse(
                amount = balance.amount,
                currency = balance.currency,
            ),
        updatedAt = updatedAtMicros.toIso8601(),
    )
