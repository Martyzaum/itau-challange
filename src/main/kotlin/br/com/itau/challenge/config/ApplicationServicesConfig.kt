package br.com.itau.challenge.config

import br.com.itau.challenge.balance.application.GetAccountBalanceService
import br.com.itau.challenge.balance.application.ProcessTransactionEventService
import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ApplicationServicesConfig {

    @Bean
    fun getAccountBalanceUseCase(accountBalanceProvider: AccountBalanceProvider): GetAccountBalanceUseCase =
        GetAccountBalanceService(accountBalanceProvider)

    @Bean
    fun processTransactionEventUseCase(
        accountBalanceRepository: AccountBalanceRepository,
    ): ProcessTransactionEventUseCase = ProcessTransactionEventService(accountBalanceRepository)
}
