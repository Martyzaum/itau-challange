package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.adapter.observability.DynamoDbObservations
import br.com.itau.challenge.balance.adapter.output.dynamodb.AccountBalanceAttributes as Attr
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import br.com.itau.challenge.config.CircuitBreakerNames
import br.com.itau.challenge.config.executeAndTranslateOpen
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.observation.ObservationRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

@Component("dynamoDbAccountBalanceRepository")
class DynamoDbAccountBalanceRepository(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.account-balances-table-name}") private val tableName: String,
    private val observationRegistry: ObservationRegistry,
    circuitBreakerRegistry: CircuitBreakerRegistry,
) : AccountBalanceRepository {

    private val dynamoDbCircuitBreaker: CircuitBreaker =
        circuitBreakerRegistry.circuitBreaker(CircuitBreakerNames.DYNAMODB_WRITE)

    override fun saveIfNewer(accountBalance: AccountBalance): Boolean =
        dynamoDbCircuitBreaker.executeAndTranslateOpen(CircuitBreakerNames.DYNAMODB_WRITE) {
            DynamoDbObservations.observePutItem(observationRegistry, accountBalance.id) {
                val request =
                    PutItemRequest
                        .builder()
                        .tableName(tableName)
                        .item(accountBalance.toItem())
                        .conditionExpression(Attr.CONDITION_SAVE_IF_NEWER)
                        .expressionAttributeNames(
                            mapOf(
                                "#accountId" to Attr.ACCOUNT_ID,
                                "#updatedAt" to Attr.UPDATED_AT_MICROS,
                                "#lastTxId" to Attr.LAST_TRANSACTION_ID,
                            ),
                        ).expressionAttributeValues(
                            mapOf(
                                ":newUpdatedAt" to
                                    AttributeValue
                                        .builder()
                                        .n(accountBalance.updatedAtMicros.toString())
                                        .build(),
                                ":newLastTxId" to
                                    AttributeValue
                                        .builder()
                                        .s(accountBalance.lastTransactionId.toString())
                                        .build(),
                            ),
                        ).build()

                try {
                    dynamoDbClient.putItem(request)
                    true
                } catch (_: ConditionalCheckFailedException) {
                    false
                }
            }
        }

    private fun AccountBalance.toItem(): Map<String, AttributeValue> =
        mapOf(
            Attr.ACCOUNT_ID to AttributeValue.builder().s(id.toString()).build(),
            Attr.OWNER to AttributeValue.builder().s(owner.toString()).build(),
            Attr.BALANCE_AMOUNT to AttributeValue.builder().n(balance.amount.toPlainString()).build(),
            Attr.BALANCE_CURRENCY to AttributeValue.builder().s(balance.currency).build(),
            Attr.UPDATED_AT_MICROS to AttributeValue.builder().n(updatedAtMicros.toString()).build(),
            Attr.LAST_TRANSACTION_ID to AttributeValue.builder().s(lastTransactionId.toString()).build(),
        )
}
