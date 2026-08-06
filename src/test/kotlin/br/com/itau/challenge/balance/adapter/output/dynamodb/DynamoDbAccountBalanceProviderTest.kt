package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.application.exception.DependencyUnavailableException
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class DynamoDbAccountBalanceProviderTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")
    private val transactionId = UUID.fromString("8e8ae808-b154-48b5-9f3e-553935cc4543")

    @Test
    fun `should get and map account balance from configured table`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(
            GetItemResponse.builder().item(accountBalanceItem()).build(),
        )
        val provider =
            DynamoDbAccountBalanceProvider(
                client,
                "AccountBalances",
                true,
                64,
                0L,
                ObservationRegistry.NOOP,
                closedRegistry(),
            )

        val accountBalance = provider.findByAccountId(accountId)

        assertEquals(accountId, accountBalance?.id)
        assertEquals(ownerId, accountBalance?.owner)
        assertEquals(BigDecimal("183.12"), accountBalance?.balance?.amount)
        assertEquals("BRL", accountBalance?.balance?.currency)
        assertEquals(1_751_641_364_589_998, accountBalance?.updatedAtMicros)
        assertEquals(transactionId, accountBalance?.lastTransactionId)
    }

    @Test
    fun `should use configured table and account id as key`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(GetItemResponse.builder().build())
        val provider =
            DynamoDbAccountBalanceProvider(
                client,
                "CustomAccountBalances",
                true,
                64,
                0L,
                ObservationRegistry.NOOP,
                closedRegistry(),
            )

        provider.findByAccountId(accountId)

        val requestCaptor = ArgumentCaptor.forClass(GetItemRequest::class.java)
        verify(client).getItem(requestCaptor.capture())
        assertEquals("CustomAccountBalances", requestCaptor.value.tableName())
        assertEquals(accountId.toString(), requestCaptor.value.key()["account_id"]?.s())
        assertEquals(true, requestCaptor.value.consistentRead())
    }

    @Test
    fun `should honor consistent read flag false`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(GetItemResponse.builder().build())
        val provider =
            DynamoDbAccountBalanceProvider(
                client,
                "AccountBalances",
                false,
                64,
                0L,
                ObservationRegistry.NOOP,
                closedRegistry(),
            )

        provider.findByAccountId(accountId)

        val requestCaptor = ArgumentCaptor.forClass(GetItemRequest::class.java)
        verify(client).getItem(requestCaptor.capture())
        assertEquals(false, requestCaptor.value.consistentRead())
    }

    @Test
    fun `should return null when account balance does not exist`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(GetItemResponse.builder().build())
        val provider =
            DynamoDbAccountBalanceProvider(
                client,
                "AccountBalances",
                true,
                64,
                0L,
                ObservationRegistry.NOOP,
                closedRegistry(),
            )

        val accountBalance = provider.findByAccountId(accountId)

        assertNull(accountBalance)
    }

    @Test
    fun `should map open circuit to dependency unavailable`() {
        val client = mock(DynamoDbClient::class.java)
        val provider =
            DynamoDbAccountBalanceProvider(
                client,
                "AccountBalances",
                true,
                64,
                0L,
                ObservationRegistry.NOOP,
                openRegistry(),
            )

        assertFailsWith<DependencyUnavailableException> {
            provider.findByAccountId(accountId)
        }
    }

    private fun closedRegistry(): CircuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()

    private fun openRegistry(): CircuitBreakerRegistry {
        val registry = CircuitBreakerRegistry.ofDefaults()
        val breaker = registry.circuitBreaker(CircuitBreakerNames.DYNAMODB_READ)
        breaker.transitionToOpenState()
        return registry
    }

    private fun accountBalanceItem(): Map<String, AttributeValue> =
        mapOf(
            "account_id" to AttributeValue.builder().s(accountId.toString()).build(),
            "owner" to AttributeValue.builder().s(ownerId.toString()).build(),
            "balance_amount" to AttributeValue.builder().n("183.12").build(),
            "balance_currency" to AttributeValue.builder().s("BRL").build(),
            "updated_at_micros" to AttributeValue.builder().n("1751641364589998").build(),
            "last_transaction_id" to AttributeValue.builder().s(transactionId.toString()).build(),
        )
}
