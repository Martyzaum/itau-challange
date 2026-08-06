package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import io.micrometer.observation.ObservationRegistry
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
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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

    private val repository =
        DynamoDbAccountBalanceRepository(
            dynamoDbClient,
            tableName,
            ObservationRegistry.NOOP,
            io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults(),
        )
    private val provider =
        DynamoDbAccountBalanceProvider(
            dynamoDbClient,
            tableName,
            true,
            64,
            0L,
            ObservationRegistry.NOOP,
            io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults(),
        )

    private lateinit var accountId: UUID
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val lowerTxId = UUID.fromString("00000000-0000-4000-8000-000000000001")
    private val higherTxId = UUID.fromString("ffffffff-ffff-4fff-8fff-ffffffffffff")

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
    fun `should ignore equal version redelivery and keep stored snapshot`() {
        assertTrue(
            repository.saveIfNewer(
                accountBalance(updatedAtMicros = 200, amount = "200.00", lastTransactionId = lowerTxId),
            ),
        )

        val saved =
            repository.saveIfNewer(
                accountBalance(updatedAtMicros = 200, amount = "999.99", lastTransactionId = lowerTxId),
            )

        assertFalse(saved)
        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(0, BigDecimal("200.00").compareTo(stored.balance.amount))
        assertEquals(200, stored.updatedAtMicros)
        assertEquals(lowerTxId, stored.lastTransactionId)
    }

    @Test
    fun `should accept distinct transaction at equal timestamp when transaction id is higher`() {
        assertTrue(
            repository.saveIfNewer(
                accountBalance(updatedAtMicros = 200, amount = "200.00", lastTransactionId = lowerTxId),
            ),
        )

        val saved =
            repository.saveIfNewer(
                accountBalance(updatedAtMicros = 200, amount = "300.00", lastTransactionId = higherTxId),
            )

        assertTrue(saved)
        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(0, BigDecimal("300.00").compareTo(stored.balance.amount))
        assertEquals(higherTxId, stored.lastTransactionId)
    }

    @Test
    fun `should ignore equal timestamp when transaction id is lower`() {
        assertTrue(
            repository.saveIfNewer(
                accountBalance(updatedAtMicros = 200, amount = "200.00", lastTransactionId = higherTxId),
            ),
        )

        val saved =
            repository.saveIfNewer(
                accountBalance(updatedAtMicros = 200, amount = "50.00", lastTransactionId = lowerTxId),
            )

        assertFalse(saved)
        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(0, BigDecimal("200.00").compareTo(stored.balance.amount))
        assertEquals(higherTxId, stored.lastTransactionId)
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

    @Test
    fun `should keep the newest snapshot under concurrent writes`() {
        assertTrue(repository.saveIfNewer(accountBalance(updatedAtMicros = 100, amount = "100.00")))

        val executor = Executors.newFixedThreadPool(8)
        try {
            val tasks =
                (1..20).map { offset ->
                    Callable {
                        repository.saveIfNewer(
                            accountBalance(
                                updatedAtMicros = 100L + offset,
                                amount = "${100 + offset}.00",
                                lastTransactionId = UUID.randomUUID(),
                            ),
                        )
                    }
                }
            executor.invokeAll(tasks).forEach { it.get(10, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }

        val stored = assertNotNull(provider.findByAccountId(accountId))
        assertEquals(120, stored.updatedAtMicros)
        assertEquals(0, BigDecimal("120.00").compareTo(stored.balance.amount))
    }

    private fun accountBalance(
        updatedAtMicros: Long,
        amount: String,
        lastTransactionId: UUID = UUID.randomUUID(),
    ): AccountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = Balance(BigDecimal(amount), "BRL"),
            updatedAtMicros = updatedAtMicros,
            lastTransactionId = lastTransactionId,
        )
}
