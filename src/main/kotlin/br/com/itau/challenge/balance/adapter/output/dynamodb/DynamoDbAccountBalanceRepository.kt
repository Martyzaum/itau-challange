package br.com.itau.challenge.balance.adapter.output.dynamodb

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceRepository
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

private const val ACCOUNT_ID_ATTRIBUTE = "account_id"
private const val OWNER_ATTRIBUTE = "owner"
private const val BALANCE_AMOUNT_ATTRIBUTE = "balance_amount"
private const val BALANCE_CURRENCY_ATTRIBUTE = "balance_currency"
private const val UPDATED_AT_MICROS_ATTRIBUTE = "updated_at_micros"
private const val CONDITION_EXPRESSION =
    "attribute_not_exists(#accountId) OR #updatedAt < :newUpdatedAt"

@Component
class DynamoDbAccountBalanceRepository(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.account-balances-table-name}") private val tableName: String,
) : AccountBalanceRepository {

    override fun saveIfNewer(accountBalance: AccountBalance): Boolean {
        val request =
            PutItemRequest
                .builder()
                .tableName(tableName)
                .item(accountBalance.toItem())
                .conditionExpression(CONDITION_EXPRESSION)
                .expressionAttributeNames(
                    mapOf(
                        "#accountId" to ACCOUNT_ID_ATTRIBUTE,
                        "#updatedAt" to UPDATED_AT_MICROS_ATTRIBUTE,
                    ),
                ).expressionAttributeValues(
                    mapOf(
                        ":newUpdatedAt" to
                            AttributeValue
                                .builder()
                                .n(accountBalance.updatedAtMicros.toString())
                                .build(),
                    ),
                ).build()

        return try {
            dynamoDbClient.putItem(request)
            true
        } catch (_: ConditionalCheckFailedException) {
            false
        }
    }

    private fun AccountBalance.toItem(): Map<String, AttributeValue> =
        mapOf(
            ACCOUNT_ID_ATTRIBUTE to AttributeValue.builder().s(id.toString()).build(),
            OWNER_ATTRIBUTE to AttributeValue.builder().s(owner.toString()).build(),
            BALANCE_AMOUNT_ATTRIBUTE to AttributeValue.builder().n(balance.amount.toPlainString()).build(),
            BALANCE_CURRENCY_ATTRIBUTE to AttributeValue.builder().s(balance.currency).build(),
            UPDATED_AT_MICROS_ATTRIBUTE to AttributeValue.builder().n(updatedAtMicros.toString()).build(),
        )
}
