package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
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
    private val accountBalance =
        AccountBalance(
            id = accountId,
            owner = ownerId,
            balance = Balance(BigDecimal("183.12"), "BRL"),
            updatedAtMicros = 1_751_641_364_589_998,
        )

    @Test
    fun `should put newer account balance into configured table`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willReturn(PutItemResponse.builder().build())
        val repository = DynamoDbAccountBalanceRepository(client, "AccountBalances")

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
        assertEquals(
            "attribute_not_exists(#accountId) OR #updatedAt < :newUpdatedAt",
            request.conditionExpression(),
        )
        assertEquals("account_id", request.expressionAttributeNames()["#accountId"])
        assertEquals("updated_at_micros", request.expressionAttributeNames()["#updatedAt"])
        assertEquals("1751641364589998", request.expressionAttributeValues()[":newUpdatedAt"]?.n())
    }

    @Test
    fun `should return false when condition rejects older or duplicate snapshot`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willThrow(
            ConditionalCheckFailedException.builder().message("condition failed").build(),
        )
        val repository = DynamoDbAccountBalanceRepository(client, "AccountBalances")

        val saved = repository.saveIfNewer(accountBalance)

        assertFalse(saved)
    }

    @Test
    fun `should propagate unexpected dynamodb failures`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.putItem(any(PutItemRequest::class.java))).willThrow(RuntimeException("boom"))
        val repository = DynamoDbAccountBalanceRepository(client, "AccountBalances")

        assertFailsWith<RuntimeException> {
            repository.saveIfNewer(accountBalance)
        }
    }
}
