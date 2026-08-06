package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.adapter.observability.DynamoDbObservations
import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.port.output.AccountBalanceProvider
import io.micrometer.observation.ObservationRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import java.math.BigDecimal
import java.util.UUID

private const val ACCOUNT_ID_ATTRIBUTE = "account_id"
private const val OWNER_ATTRIBUTE = "owner"
private const val BALANCE_AMOUNT_ATTRIBUTE = "balance_amount"
private const val BALANCE_CURRENCY_ATTRIBUTE = "balance_currency"
private const val UPDATED_AT_MICROS_ATTRIBUTE = "updated_at_micros"
private const val LAST_TRANSACTION_ID_ATTRIBUTE = "last_transaction_id"

@Component
class DynamoDbAccountBalanceProvider(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.account-balances-table-name}") private val tableName: String,
    private val observationRegistry: ObservationRegistry,
) : AccountBalanceProvider {

    override fun findByAccountId(accountId: UUID): AccountBalance? =
        DynamoDbObservations.observeGetItem(observationRegistry, accountId) {
            val request =
                GetItemRequest
                    .builder()
                    .tableName(tableName)
                    .consistentRead(true)
                    .key(
                        mapOf(
                            ACCOUNT_ID_ATTRIBUTE to AttributeValue.builder().s(accountId.toString()).build(),
                        ),
                    ).build()

            val response = dynamoDbClient.getItem(request)
            if (!response.hasItem()) {
                return@observeGetItem null
            }

            response.item().toAccountBalance()
        }

    private fun Map<String, AttributeValue>.toAccountBalance(): AccountBalance =
        AccountBalance(
            id = UUID.fromString(getValue(ACCOUNT_ID_ATTRIBUTE).s()),
            owner = UUID.fromString(getValue(OWNER_ATTRIBUTE).s()),
            balance =
                Balance(
                    amount = BigDecimal(getValue(BALANCE_AMOUNT_ATTRIBUTE).n()),
                    currency = getValue(BALANCE_CURRENCY_ATTRIBUTE).s(),
                ),
            updatedAtMicros = getValue(UPDATED_AT_MICROS_ATTRIBUTE).n().toLong(),
            lastTransactionId = UUID.fromString(getValue(LAST_TRANSACTION_ID_ATTRIBUTE).s()),
        )
}
