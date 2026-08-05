package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import java.math.BigDecimal
import java.net.URI
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Exercises conditional writes against a real DynamoDB Local instance
 * (see `make db-up`). Not part of `./gradlew test`/`check`; run with
 * `./gradlew integrationTest` or `make integration-test`.
 */
class DynamoDbAccountBalanceRepositoryIntegrationTest {

    private val tableName = System.getenv("ACCOUNT_BALANCES_TABLE_NAME") ?: "AccountBalances"

    private val dynamoDbClient: DynamoDbClient =
        DynamoDbClient
            .builder()
            .endpointOverride(URI.create(System.getenv("DYNAMODB_ENDPOINT") ?: "http://localhost:8000"))
            .region(Region.of(System.getenv("DYNAMODB_REGION") ?: "us-east-1"))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
            .build()

    private val repository = DynamoDbAccountBalanceRepository(dynamoDbClient, tableName)
    private val provider = DynamoDbAccountBalanceProvider(dynamoDbClient, tableName)

    private lateinit var accountId: UUID
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")

    @BeforeEach
    fun setUp() {
        accountId = UUID.randomUUID()
    }

    @AfterEach
    fun tearDown() {
        dynamoDbClient.deleteItem(
            DeleteItemRequest
                .builder()
                .tableName(tableName)
                .key(mapOf("account_id" to AttributeValue.builder().s(accountId.toString()).build()))
                .build(),
        )
    }

    @Test
    fun `should ignore equal timestamp as duplicate and keep stored snapshot`() {
        assertTrue(repository.saveIfNewer(accountBalance(updatedAtMicros = 200, amount = "200.00")))

        val saved = repository.saveIfNewer(accountBalance(updatedAtMicros = 200, amount = "999.99"))

        assertFalse(saved)
        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(0, BigDecimal("200.00").compareTo(stored.balance.amount))
        assertEquals(200, stored.updatedAtMicros)
    }

    @Test
    fun `should ignore older timestamp and keep newer snapshot`() {
        assertTrue(repository.saveIfNewer(accountBalance(updatedAtMicros = 200, amount = "200.00")))

        val saved = repository.saveIfNewer(accountBalance(updatedAtMicros = 100, amount = "100.00"))

        assertFalse(saved)
        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(0, BigDecimal("200.00").compareTo(stored.balance.amount))
        assertEquals(200, stored.updatedAtMicros)
    }

    @Test
    fun `should overwrite only when incoming timestamp is newer`() {
        assertTrue(repository.saveIfNewer(accountBalance(updatedAtMicros = 200, amount = "200.00")))

        val saved = repository.saveIfNewer(accountBalance(updatedAtMicros = 300, amount = "300.00"))

        assertTrue(saved)
        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(0, BigDecimal("300.00").compareTo(stored.balance.amount))
        assertEquals(300, stored.updatedAtMicros)
    }

    private fun accountBalance(
        updatedAtMicros: Long,
        amount: String,
    ): AccountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = Balance(BigDecimal(amount), "BRL"),
            updatedAtMicros = updatedAtMicros,
        )
}
