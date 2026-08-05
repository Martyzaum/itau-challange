package br.com.itau.challenge.balance.adapter.output.dynamodb

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
import kotlin.test.assertNull

class DynamoDbAccountBalanceProviderTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")

    @Test
    fun `should get and map account balance from configured table`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(
            GetItemResponse.builder().item(accountBalanceItem()).build(),
        )
        val provider = DynamoDbAccountBalanceProvider(client, "AccountBalances")

        val accountBalance = provider.findByAccountId(accountId)

        assertEquals(accountId, accountBalance?.id)
        assertEquals(ownerId, accountBalance?.owner)
        assertEquals(BigDecimal("183.12"), accountBalance?.balance?.amount)
        assertEquals("BRL", accountBalance?.balance?.currency)
        assertEquals(1_751_641_364_589_998, accountBalance?.updatedAtMicros)
    }

    @Test
    fun `should use configured table and account id as key`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(GetItemResponse.builder().build())
        val provider = DynamoDbAccountBalanceProvider(client, "CustomAccountBalances")

        provider.findByAccountId(accountId)

        val requestCaptor = ArgumentCaptor.forClass(GetItemRequest::class.java)
        verify(client).getItem(requestCaptor.capture())
        assertEquals("CustomAccountBalances", requestCaptor.value.tableName())
        assertEquals(accountId.toString(), requestCaptor.value.key()["account_id"]?.s())
        assertEquals(true, requestCaptor.value.consistentRead())
    }

    @Test
    fun `should return null when account balance does not exist`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.getItem(any(GetItemRequest::class.java))).willReturn(GetItemResponse.builder().build())
        val provider = DynamoDbAccountBalanceProvider(client, "AccountBalances")

        val accountBalance = provider.findByAccountId(accountId)

        assertNull(accountBalance)
    }

    private fun accountBalanceItem(): Map<String, AttributeValue> =
        mapOf(
            "account_id" to AttributeValue.builder().s(accountId.toString()).build(),
            "owner" to AttributeValue.builder().s(ownerId.toString()).build(),
            "balance_amount" to AttributeValue.builder().n("183.12").build(),
            "balance_currency" to AttributeValue.builder().s("BRL").build(),
            "updated_at_micros" to AttributeValue.builder().n("1751641364589998").build(),
        )
}
