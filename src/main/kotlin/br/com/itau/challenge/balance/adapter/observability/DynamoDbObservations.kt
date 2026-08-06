package br.com.itau.challenge.balance.adapter.observability

import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationRegistry
import java.util.UUID

/**
 * Micrometer Observations for DynamoDB calls — become OTel spans via tracing bridge.
 */
internal object DynamoDbObservations {
    private const val DB_SYSTEM = "dynamodb"

    fun <T> observeGetItem(
        observationRegistry: ObservationRegistry,
        accountId: UUID,
        block: () -> T,
    ): T =
        Observation
            .createNotStarted("dynamodb.get_item", observationRegistry)
            .lowCardinalityKeyValue("db.system", DB_SYSTEM)
            .lowCardinalityKeyValue("db.operation", "GetItem")
            .highCardinalityKeyValue("account.id", accountId.toString())
            .observe(block)

    fun <T> observePutItem(
        observationRegistry: ObservationRegistry,
        accountId: UUID,
        block: () -> T,
    ): T =
        Observation
            .createNotStarted("dynamodb.put_item", observationRegistry)
            .lowCardinalityKeyValue("db.system", DB_SYSTEM)
            .lowCardinalityKeyValue("db.operation", "PutItem")
            .highCardinalityKeyValue("account.id", accountId.toString())
            .observe(block)
}
