package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.adapter.observability.DynamoDbObservations
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import br.com.itau.challenge.config.CircuitBreakerNames
import br.com.itau.challenge.config.executeAndTranslateOpen
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.observation.ObservationRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import java.math.BigDecimal
import java.util.UUID
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import br.com.itau.challenge.balance.application.exception.DependencyUnavailableException
import br.com.itau.challenge.balance.adapter.output.dynamodb.AccountBalanceAttributes as Attr

@Component("dynamoDbAccountBalanceProvider")
class DynamoDbAccountBalanceProvider(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.account-balances-table-name}") private val tableName: String,
    @Value("\${dynamodb.consistent-read}") private val consistentRead: Boolean,
    @Value("\${dynamodb.read-bulkhead-max-concurrent:64}") private val readBulkheadMaxConcurrent: Int,
    @Value("\${dynamodb.read-bulkhead-timeout-ms:50}") private val readBulkheadTimeoutMs: Long,
    private val observationRegistry: ObservationRegistry,
    circuitBreakerRegistry: CircuitBreakerRegistry,
) : AccountBalanceProvider {

    private val dynamoDbCircuitBreaker: CircuitBreaker =
        circuitBreakerRegistry.circuitBreaker(CircuitBreakerNames.DYNAMODB_READ)
    private val readBulkhead = Semaphore(readBulkheadMaxConcurrent.coerceAtLeast(1))

    override fun findByAccountId(accountId: UUID): AccountBalance? {
        val acquired =
            try {
                readBulkhead.tryAcquire(readBulkheadTimeoutMs.coerceAtLeast(0), TimeUnit.MILLISECONDS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }
        if (!acquired) {
            throw DependencyUnavailableException(CircuitBreakerNames.DYNAMODB_READ)
        }
        return try {
            dynamoDbCircuitBreaker.executeAndTranslateOpen(CircuitBreakerNames.DYNAMODB_READ) {
                DynamoDbObservations.observeGetItem(observationRegistry, accountId) {
                    val request =
                        GetItemRequest
                            .builder()
                            .tableName(tableName)
                            .consistentRead(consistentRead)
                            .key(
                                mapOf(
                                    Attr.ACCOUNT_ID to AttributeValue.builder().s(accountId.toString()).build(),
                                ),
                            ).build()

                    val response = dynamoDbClient.getItem(request)
                    if (!response.hasItem()) {
                        return@observeGetItem null
                    }

                    response.item().toAccountBalance()
                }
            }
        } finally {
            readBulkhead.release()
        }
    }

    private fun Map<String, AttributeValue>.toAccountBalance(): AccountBalance =
        AccountBalance(
            id = UUID.fromString(getValue(Attr.ACCOUNT_ID).s()),
            owner = UUID.fromString(getValue(Attr.OWNER).s()),
            balance =
                Balance(
                    amount = BigDecimal(getValue(Attr.BALANCE_AMOUNT).n()),
                    currency = getValue(Attr.BALANCE_CURRENCY).s(),
                ),
            updatedAtMicros = getValue(Attr.UPDATED_AT_MICROS).n().toLong(),
            lastTransactionId = UUID.fromString(getValue(Attr.LAST_TRANSACTION_ID).s()),
        )
}
