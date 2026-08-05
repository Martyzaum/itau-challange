package br.com.itau.challenge.balance.support

import java.math.BigDecimal
import java.util.UUID

/**
 * Shared JSON payloads for balance Kafka → DynamoDB → REST integration tests.
 */
object FinancialTransactionEventFixtures {
    const val TOPIC = "transacoes-financeiras-processadas"
    const val DLT_TOPIC = "transacoes-financeiras-processadas.DLT"
    const val TABLE = "AccountBalances"

    fun eligibleEventJson(
        accountId: UUID,
        ownerId: UUID = UUID.randomUUID(),
        transactionId: UUID = UUID.randomUUID(),
        balanceAmount: BigDecimal = BigDecimal("183.12"),
        transactionAmount: BigDecimal = BigDecimal("97.07"),
        transactionType: String = "CREDIT",
        transactionStatus: String = "APPROVED",
        accountStatus: String = "ENABLED",
        timestampMicros: Long = 1_751_641_364_589_998L,
        accountCreatedAtMicros: Long = 1_634_874_339_000_000L,
        currency: String = "BRL",
    ): String =
        """
        {
          "transaction": {
            "id": "$transactionId",
            "type": "$transactionType",
            "amount": $transactionAmount,
            "currency": "$currency",
            "status": "$transactionStatus",
            "timestamp": $timestampMicros
          },
          "account": {
            "id": "$accountId",
            "owner": "$ownerId",
            "created_at": $accountCreatedAtMicros,
            "status": "$accountStatus",
            "balance": {
              "amount": $balanceAmount,
              "currency": "$currency"
            }
          }
        }
        """.trimIndent()

    fun invalidJsonPayload(): String = "{ not-json"

    fun invalidDomainPayload(
        accountId: UUID = UUID.randomUUID(),
        ownerId: UUID = UUID.randomUUID(),
        transactionId: UUID = UUID.randomUUID(),
    ): String =
        eligibleEventJson(
            accountId = accountId,
            ownerId = ownerId,
            transactionId = transactionId,
            transactionType = "TRANSFER",
        )
}
