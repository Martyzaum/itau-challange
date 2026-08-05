package br.com.itau.challenge.balance.adapter.observability

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.health.contributor.AbstractHealthIndicator
import org.springframework.boot.health.contributor.Health
import org.springframework.stereotype.Component
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest

@Component
class DynamoDbHealthIndicator(
    private val dynamoDbClient: DynamoDbClient,
    @Value("\${dynamodb.account-balances-table-name}") private val accountBalancesTableName: String,
) : AbstractHealthIndicator("DynamoDB health check failed") {

    override fun doHealthCheck(builder: Health.Builder) {
        val table =
            dynamoDbClient
                .describeTable(
                    DescribeTableRequest
                        .builder()
                        .tableName(accountBalancesTableName)
                        .build(),
                ).table()

        builder
            .up()
            .withDetail("table", table.tableName())
            .withDetail("status", table.tableStatusAsString())
    }
}
