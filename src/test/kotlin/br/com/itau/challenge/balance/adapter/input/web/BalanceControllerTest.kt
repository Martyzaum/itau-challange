package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.domain.exception.AccountBalanceNotFoundException
import br.com.itau.challenge.balance.domain.exception.DependencyUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import br.com.itau.challenge.config.CircuitBreakerNames
import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import java.math.BigDecimal
import java.util.UUID

@SpringBootTest
@AutoConfigureMockMvc
class BalanceControllerTest(
    @Autowired private val mockMvc: MockMvc,
) {

    @MockitoBean
    private lateinit var getAccountBalanceUseCase: GetAccountBalanceUseCase

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")

    @Test
    fun `should return account balance payload`() {
        given(getAccountBalanceUseCase.getAccountBalance(accountId)).willReturn(
            AccountBalance(
                id = accountId,
                owner = ownerId,
                balance = Balance(BigDecimal("183.12"), "BRL"),
                updatedAtMicros = 1_751_641_364_589_998,
                lastTransactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
            ),
        )

        mockMvc.get("/balances/$accountId").andExpect {
            status { isOk() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.id") { value(accountId.toString()) }
            jsonPath("$.owner") { value(ownerId.toString()) }
            jsonPath("$.balance.amount") { value(183.12) }
            jsonPath("$.balance.currency") { value("BRL") }
            jsonPath("$.updated_at") { exists() }
        }
    }

    @Test
    fun `should return not found when account balance does not exist`() {
        given(getAccountBalanceUseCase.getAccountBalance(accountId))
            .willThrow(AccountBalanceNotFoundException(accountId))

        mockMvc.get("/balances/$accountId").andExpect {
            status { isNotFound() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.code") { value("ACCOUNT_BALANCE_NOT_FOUND") }
            jsonPath("$.message") { value("Account balance not found for account $accountId") }
        }
    }

    @Test
    fun `should reject invalid account id uuid`() {
        mockMvc.get("/balances/not-a-uuid").andExpect {
            status { isBadRequest() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.code") { value("INVALID_ACCOUNT_ID") }
            jsonPath("$.message") { value("accountId must be a valid UUID") }
        }
    }

    @Test
    fun `should return service unavailable when dynamodb circuit is open`() {
        given(getAccountBalanceUseCase.getAccountBalance(accountId))
            .willThrow(DependencyUnavailableException(CircuitBreakerNames.DYNAMODB))

        mockMvc.get("/balances/$accountId").andExpect {
            status { isServiceUnavailable() }
            content { contentType(MediaType.APPLICATION_JSON) }
            jsonPath("$.code") { value("DEPENDENCY_UNAVAILABLE") }
            jsonPath("$.message") { value("Dependency unavailable: dynamodb") }
        }
    }
}
