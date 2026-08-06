package br.com.itau.challenge.balance.adapter.input.web.security

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.port.input.GetAccountBalanceUseCase
import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import java.math.BigDecimal
import java.util.UUID

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(
    properties = [
        "api.auth.enabled=true",
        "api.auth.keys=test-secret-key",
        "api.rate-limit.enabled=true",
        "api.rate-limit.requests-per-minute=3",
    ],
)
class ApiSecurityFiltersTest(
    @Autowired private val mockMvc: MockMvc,
) {

    @MockitoBean
    private lateinit var getAccountBalanceUseCase: GetAccountBalanceUseCase

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")

    @Test
    fun `should reject balance request without api key`() {
        mockMvc.get("/balances/$accountId").andExpect {
            status { isUnauthorized() }
            content { contentTypeCompatibleWith(MediaType.APPLICATION_JSON) }
            jsonPath("$.code") { value("UNAUTHORIZED") }
        }
    }

    @Test
    fun `should reject balance request with invalid api key`() {
        mockMvc
            .get("/balances/$accountId") {
                header("X-API-Key", "wrong")
            }.andExpect {
                status { isUnauthorized() }
                jsonPath("$.code") { value("UNAUTHORIZED") }
            }
    }

    @Test
    fun `should allow health without api key`() {
        mockMvc.get("/actuator/health/liveness").andExpect {
            status { isOk() }
        }
    }

    @Test
    fun `should return balance with valid api key and enforce rate limit`() {
        given(getAccountBalanceUseCase.getAccountBalance(accountId)).willReturn(
            AccountBalance(
                id = accountId,
                owner = ownerId,
                balance = Balance(BigDecimal("10.00"), "BRL"),
                updatedAtMicros = 100L,
                lastTransactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543"),
            ),
        )

        repeat(3) {
            mockMvc
                .get("/balances/$accountId") {
                    header("X-API-Key", "test-secret-key")
                }.andExpect {
                    status { isOk() }
                    header { exists("X-RateLimit-Limit") }
                }
        }

        mockMvc
            .get("/balances/$accountId") {
                header("X-API-Key", "test-secret-key")
            }.andExpect {
                status { isTooManyRequests() }
                content { contentTypeCompatibleWith(MediaType.APPLICATION_JSON) }
                jsonPath("$.code") { value("RATE_LIMIT_EXCEEDED") }
                header { exists("Retry-After") }
            }
    }
}
