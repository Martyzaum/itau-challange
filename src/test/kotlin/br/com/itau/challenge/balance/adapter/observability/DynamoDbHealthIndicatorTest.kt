package br.com.itau.challenge.balance.adapter.observability

import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.springframework.boot.health.contributor.Status
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DynamoDbHealthIndicatorTest {

    @Test
    fun `should report up when account balances table is active`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.describeTable(any(DescribeTableRequest::class.java))).willReturn(
            DescribeTableResponse
                .builder()
                .table(
                    TableDescription
                        .builder()
                        .tableName("AccountBalances")
                        .tableStatus(TableStatus.ACTIVE)
                        .build(),
                ).build(),
        )

        val health = DynamoDbHealthIndicator(client, "AccountBalances").health()

        assertEquals(Status.UP, health.status)
        assertEquals("AccountBalances", health.details["table"])
        assertEquals("ACTIVE", health.details["status"])
    }

    @Test
    fun `should report down when dynamodb call fails`() {
        val client = mock(DynamoDbClient::class.java)
        given(client.describeTable(any(DescribeTableRequest::class.java))).willThrow(
            ResourceNotFoundException.builder().message("missing").build(),
        )

        val health = DynamoDbHealthIndicator(client, "AccountBalances").health()

        assertEquals(Status.DOWN, health.status)
        assertTrue(health.details.containsKey("error"))
    }
}
