package br.com.itau.challenge.balance

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
import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit

private const val TOPIC = "transacoes-financeiras-processadas"
private const val TABLE = "AccountBalances"

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
        kafkaTemplate
            .send(
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
            ).get(10, TimeUnit.SECONDS)

        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        var lastError: Throwable? = null
        while (System.nanoTime() < deadline) {
            try {
                mockMvc.get("/balances/$accountId").andExpect {
                    status { isOk() }
                    content { contentType(MediaType.APPLICATION_JSON) }
                    jsonPath("$.id") { value(accountId.toString()) }
                    jsonPath("$.owner") { value(ownerId.toString()) }
                    jsonPath("$.balance.amount") { value(183.12) }
                    jsonPath("$.balance.currency") { value("BRL") }
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
