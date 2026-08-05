package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.junit.jupiter.api.Test
import org.mockito.Mockito.timeout
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import java.math.BigDecimal
import java.util.UUID

private const val TOPIC = "transacoes-financeiras-processadas"

/**
 * Boots the real Spring context and [TransactionEventConsumer] against the real Redpanda
 * broker (`make kafka-up`). Only the DynamoDB repository port is mocked.
 */
@SpringBootTest
class TransactionEventConsumerIntegrationTest {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockitoBean
    private lateinit var accountBalanceRepository: AccountBalanceRepository

    @Test
    fun `should consume a transaction event from the real topic and persist if newer`() {
        val accountId = UUID.randomUUID()
        val ownerId = UUID.randomUUID()
        val transactionId = UUID.randomUUID()
        val expected =
            AccountBalance(
                id = accountId,
                owner = ownerId,
                balance = Balance(BigDecimal("183.12"), "BRL"),
                updatedAtMicros = 1_751_641_364_589_998,
            )

        kafkaTemplate.send(
            TOPIC,
            accountId.toString(),
            """
            {
              "transaction": {
                "id": "$transactionId",
                "type": "CREDIT",
                "amount": 97.07,
                "currency": "BRL",
                "status": "APPROVED",
                "timestamp": 1751641364589998
              },
              "account": {
                "id": "$accountId",
                "owner": "$ownerId",
                "created_at": 1634874339000000,
                "status": "ENABLED",
                "balance": {
                  "amount": 183.12,
                  "currency": "BRL"
                }
              }
            }
            """.trimIndent(),
        )

        verify(accountBalanceRepository, timeout(10_000)).saveIfNewer(expected)
    }
}
