package br.com.itau.challenge.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class DynamoDbConfigTest {

    @Test
    fun `should use endpoint override for local dynamodb when endpoint is present`() {
        val settings =
            resolveDynamoDbConnectionSettings(
                endpoint = "http://localhost:8000",
                region = "us-east-1",
            )

        assertEquals("us-east-1", settings.region)
        assertEquals("http://localhost:8000", settings.endpointOverride)
        assertEquals(5_000L, settings.apiCallTimeoutMs)
        assertEquals(3_000L, settings.apiCallAttemptTimeoutMs)
    }

    @Test
    fun `should trim endpoint before applying override`() {
        val settings =
            resolveDynamoDbConnectionSettings(
                endpoint = "  http://dynamodb:8000  ",
                region = "sa-east-1",
            )

        assertEquals("http://dynamodb:8000", settings.endpointOverride)
        assertEquals("sa-east-1", settings.region)
    }

    @Test
    fun `should omit endpoint override for real aws when endpoint is blank`() {
        val settings =
            resolveDynamoDbConnectionSettings(
                endpoint = "   ",
                region = "us-east-1",
            )

        assertNull(settings.endpointOverride)
        assertEquals("us-east-1", settings.region)
    }

    @Test
    fun `should omit endpoint override when endpoint is null`() {
        val settings =
            resolveDynamoDbConnectionSettings(
                endpoint = null,
                region = "us-west-2",
            )

        assertNull(settings.endpointOverride)
        assertEquals("us-west-2", settings.region)
    }

    @Test
    fun `should carry explicit api call timeouts`() {
        val settings =
            resolveDynamoDbConnectionSettings(
                endpoint = "http://localhost:8000",
                region = "us-east-1",
                apiCallTimeoutMs = 2_000L,
                apiCallAttemptTimeoutMs = 1_000L,
            )

        assertEquals(2_000L, settings.apiCallTimeoutMs)
        assertEquals(1_000L, settings.apiCallAttemptTimeoutMs)
    }

    @Test
    fun `should build local client when endpoint is configured`() {
        val client = DynamoDbConfig().dynamoDbClient("http://localhost:8000", "us-east-1", 5_000L, 3_000L)
        client.close()
    }

    @Test
    fun `should build aws client when endpoint is blank`() {
        val client = DynamoDbConfig().dynamoDbClient("", "us-east-1", 5_000L, 3_000L)
        client.close()
    }
}
