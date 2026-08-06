package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.domain.model.ProcessTransactionResult
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

/**
 * Main + retry-topic listeners share the same processing path.
 * Delay between retry levels is non-blocking: messages hop main → retry-N via the async recoverer
 * without Thread.sleep on the consumer thread (avoids HOL blocking).
 */
@Component
@ConditionalOnProperty(
    prefix = "transactions.ingestion",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = true,
)
class TransactionEventConsumer(
    private val processTransactionEventUseCase: ProcessTransactionEventUseCase,
    private val objectMapper: ObjectMapper,
    private val balanceMetrics: BalanceMetrics,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${transactions.topic-name}"])
    fun consume(payload: String) {
        processPayload(payload)
    }

    @KafkaListener(topics = ["#{@transactionRetryTopics}"])
    fun consumeRetry(payload: String) {
        processPayload(payload)
    }

    private fun processPayload(payload: String) {
        val message = objectMapper.readValue(payload, FinancialTransactionMessage::class.java)
        val event = message.toDomain()
        when (processTransactionEventUseCase.processTransactionEvent(event)) {
            ProcessTransactionResult.Saved -> {
                balanceMetrics.incrementTransactionSaved()
                logger.info(
                    "event=transaction_processed accountId={} transactionId={} result=saved",
                    event.accountId,
                    event.transactionId,
                )
            }
            ProcessTransactionResult.IgnoredIneligible,
            ProcessTransactionResult.IgnoredNotNewer,
            -> {
                balanceMetrics.incrementTransactionIgnored()
                logger.info(
                    "event=transaction_processed accountId={} transactionId={} result=ignored",
                    event.accountId,
                    event.transactionId,
                )
            }
        }
    }
}
