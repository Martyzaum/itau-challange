package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.web.dto.BalanceResponse
import br.com.itau.challenge.balance.adapter.input.web.dto.MoneyResponse
import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.domain.exception.AccountBalanceNotFoundException
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
class BalanceController(
    private val getAccountBalanceUseCase: GetAccountBalanceUseCase,
    private val balanceMetrics: BalanceMetrics,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @GetMapping("/balances/{accountId}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getBalance(@PathVariable accountId: UUID): BalanceResponse =
        try {
            val balance = getAccountBalanceUseCase.getAccountBalance(accountId).toResponse()
            balanceMetrics.incrementBalanceFound()
            logger.info("event=balance_queried accountId={} result=found", accountId)
            balance
        } catch (exception: AccountBalanceNotFoundException) {
            balanceMetrics.incrementBalanceNotFound()
            logger.info("event=balance_queried accountId={} result=not_found", accountId)
            throw exception
        }
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
