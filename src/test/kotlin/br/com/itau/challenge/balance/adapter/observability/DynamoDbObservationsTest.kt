package br.com.itau.challenge.balance.adapter.observability

import io.micrometer.observation.tck.TestObservationRegistry
import io.micrometer.observation.tck.TestObservationRegistryAssert
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals

class DynamoDbObservationsTest {

    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")

    @Test
    fun `should record get item observation`() {
        val registry = TestObservationRegistry.create()

        val result =
            DynamoDbObservations.observeGetItem(registry, accountId) {
                "ok"
            }

        assertEquals("ok", result)
        TestObservationRegistryAssert
            .assertThat(registry)
            .hasObservationWithNameEqualTo("dynamodb.get_item")
            .that()
            .hasLowCardinalityKeyValue("db.system", "dynamodb")
            .hasLowCardinalityKeyValue("db.operation", "GetItem")
            .hasHighCardinalityKeyValue("account.id", accountId.toString())
            .hasBeenStarted()
            .hasBeenStopped()
    }

    @Test
    fun `should record put item observation`() {
        val registry = TestObservationRegistry.create()

        val result =
            DynamoDbObservations.observePutItem(registry, accountId) {
                true
            }

        assertEquals(true, result)
        TestObservationRegistryAssert
            .assertThat(registry)
            .hasObservationWithNameEqualTo("dynamodb.put_item")
            .that()
            .hasLowCardinalityKeyValue("db.operation", "PutItem")
            .hasBeenStarted()
            .hasBeenStopped()
    }
}
