package br.com.itau.challenge

import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertNotNull

@SpringBootTest
class ApplicationTests {

    @Autowired
    private lateinit var getAccountBalanceUseCase: GetAccountBalanceUseCase

    @Autowired
    private lateinit var processTransactionEventUseCase: ProcessTransactionEventUseCase

    @Autowired
    private lateinit var accountBalanceProvider: AccountBalanceProvider

    @Autowired
    private lateinit var accountBalanceRepository: AccountBalanceRepository

    @Test
    fun contextLoads() {
        assertNotNull(getAccountBalanceUseCase)
        assertNotNull(processTransactionEventUseCase)
        assertNotNull(accountBalanceProvider)
        assertNotNull(accountBalanceRepository)
    }
}
