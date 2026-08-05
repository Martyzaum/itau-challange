package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TABLE
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.TOPIC
import br.com.itau.challenge.balance.support.FinancialTransactionEventFixtures.eligibleEventJson
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import java.math.BigDecimal
import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Full path: publish Kafka event → consumer persists to DynamoDB → GET /balances/{accountId}.
 * Requires live DynamoDB Local + Redpanda (`make db-up` and `make kafka-up`).
 */
@SpringBootTest
@AutoConfigureMockMvc
class BalanceEndToEndIntegrationTest(
    @Autowired private val mockMvc: MockMvc,
    @Autowired private val kafkaTemplate: KafkaTemplate<String, String>,
) {

    private lateinit var accountId: UUID
    private lateinit var ownerId: UUID
    private lateinit var transactionId: UUID

    private val dynamoDbClient: DynamoDbClient =
        DynamoDbClient
            .builder()
            .endpointOverride(URI.create(System.getenv("DYNAMODB_ENDPOINT") ?: "http://localhost:8000"))
            .region(Region.of(System.getenv("DYNAMODB_REGION") ?: "us-east-1"))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
            .build()

    @BeforeEach
    fun setUp() {
        accountId = UUID.randomUUID()
        ownerId = UUID.randomUUID()
        transactionId = UUID.randomUUID()
    }

    @AfterEach
    fun tearDown() {
        dynamoDbClient.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(TABLE)
                .key(mapOf("account_id" to AttributeValue.builder().s(accountId.toString()).build()))
                .build(),
        )
    }

    @Test
    fun `should ingest kafka event persist balance and expose it via rest`() {
        publish(
            eligibleEventJson(
                accountId = accountId,
                ownerId = ownerId,
                transactionId = transactionId,
                balanceAmount = BigDecimal("183.12"),
                timestampMicros = 1_751_641_364_589_998L,
            ),
        )

        awaitBalance(
            expectedOwnerId = ownerId,
            expectedAmount = 183.12,
        )
    }

    private fun publish(payload: String) {
        kafkaTemplate
            .send(TOPIC, accountId.toString(), payload)
            .get(10, TimeUnit.SECONDS)
    }

    private fun awaitBalance(
        expectedOwnerId: UUID,
        expectedAmount: Double,
        currency: String = "BRL",
        timeoutSeconds: Long = 20,
    ) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
        var lastError: Throwable? = null
        while (System.nanoTime() < deadline) {
            try {
                mockMvc.get("/balances/$accountId").andExpect {
                    status { isOk() }
                    content { contentType(MediaType.APPLICATION_JSON) }
                    jsonPath("$.id") { value(accountId.toString()) }
                    jsonPath("$.owner") { value(expectedOwnerId.toString()) }
                    jsonPath("$.balance.amount") { value(expectedAmount) }
                    jsonPath("$.balance.currency") { value(currency) }
                    jsonPath("$.updated_at") { exists() }
                }
                return
            } catch (error: Throwable) {
                lastError = error
                Thread.sleep(250)
            }
        }
        throw AssertionError("Balance was not available via REST within timeout", lastError)
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun kafkaConsumerGroup(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.consumer.group-id") {
                "balance-e2e-${UUID.randomUUID()}"
            }
            registry.add("management.otlp.metrics.export.enabled") { "false" }
        }
    }
}
