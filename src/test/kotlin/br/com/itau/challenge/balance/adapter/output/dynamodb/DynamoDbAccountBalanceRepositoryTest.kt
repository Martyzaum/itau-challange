package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.application.exception.DependencyUnavailableException
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.config.CircuitBreakerNames
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.observation.ObservationRegistry
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.BDDMockito.given
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DynamoDbAccountBalanceRepositoryTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val transactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")
    private val accountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = Balance(BigDecimal("183.12"), "BRL"),
            updatedAtMicros = 1_751_641_364_589_998,
            lastTransactionId = transactionId,
        )

    @Test
    fun `should put newer account balance into configured table`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willReturn(PutItemResponse.builder().build())
        val repository =
            DynamoDbAccountBalanceRepository(client, "AccountBalances", ObservationRegistry.NOOP, closedRegistry())

        val saved = repository.saveIfNewer(accountBalance)

        assertTrue(saved)
        val requestCaptor = ArgumentCaptor.forClass(PutItemRequest::class.java)
        verify(client).putItem(requestCaptor.capture())
        val request = requestCaptor.value
        assertEquals("AccountBalances", request.tableName())
        assertEquals(accountId.toString(), request.item()["account_id"]?.s())
        assertEquals(ownerId.toString(), request.item()["owner"]?.s())
        assertEquals("183.12", request.item()["balance_amount"]?.n())
        assertEquals("BRL", request.item()["balance_currency"]?.s())
        assertEquals("1751641364589998", request.item()["updated_at_micros"]?.n())
        assertEquals(transactionId.toString(), request.item()["last_transaction_id"]?.s())
        assertTrue(request.conditionExpression().contains("#updatedAt < :newUpdatedAt"))
        assertTrue(request.conditionExpression().contains("#lastTxId < :newLastTxId"))
        assertEquals("account_id", request.expressionAttributeNames()["#accountId"])
        assertEquals("updated_at_micros", request.expressionAttributeNames()["#updatedAt"])
        assertEquals("last_transaction_id", request.expressionAttributeNames()["#lastTxId"])
        assertEquals("1751641364589998", request.expressionAttributeValues()[":newUpdatedAt"]?.n())
        assertEquals(transactionId.toString(), request.expressionAttributeValues()[":newLastTxId"]?.s())
    }

    @Test
    fun `should treat condition failure as ignored snapshot`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willThrow(
            ConditionalCheckFailedException.builder().message("condition failed").build(),
        )
        val repository =
            DynamoDbAccountBalanceRepository(client, "AccountBalances", ObservationRegistry.NOOP, closedRegistry())

        val saved = repository.saveIfNewer(accountBalance)

        assertFalse(saved)
    }

    @Test
    fun `should reject older timestamp without overwrite when condition fails`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willThrow(
            ConditionalCheckFailedException.builder().message("condition failed").build(),
        )
        val repository =
            DynamoDbAccountBalanceRepository(client, "AccountBalances", ObservationRegistry.NOOP, closedRegistry())

        val saved =
            repository.saveIfNewer(
                accountBalance.copy(updatedAtMicros = accountBalance.updatedAtMicros - 1),
            )

        assertFalse(saved)
    }

    @Test
    fun `should propagate unexpected dynamodb failures`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willThrow(RuntimeException("boom"))
        val repository =
            DynamoDbAccountBalanceRepository(client, "AccountBalances", ObservationRegistry.NOOP, closedRegistry())

        assertFailsWith<RuntimeException> {
            repository.saveIfNewer(accountBalance)
        }
    }

    @Test
    fun `should map open circuit to dependency unavailable for write path`() {
        val client = mock(DynamoDbClient::class.java)
        val repository =
            DynamoDbAccountBalanceRepository(client, "AccountBalances", ObservationRegistry.NOOP, openRegistry())

        assertFailsWith<DependencyUnavailableException> {
            repository.saveIfNewer(accountBalance)
        }
    }

    private fun closedRegistry(): CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()

    private fun openRegistry(): CircuitBreakerRegistry {
        val registry = CircuitBreakerRegistry.ofDefaults()
        registry.circuitBreaker(CircuitBreakerNames.DYNAMODB_WRITE).transitionToOpenState()
        return registry
    }
}
