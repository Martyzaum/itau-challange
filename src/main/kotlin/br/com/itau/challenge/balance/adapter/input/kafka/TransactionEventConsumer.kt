package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.domain.model.ProcessTransactionResult
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

/**
 * Main + retry-topic listeners share the same processing path.
 * Main topic never sleeps. Retry listeners honor x-retry-not-before-ms
 * (exp backoff + full jitter set by the async recoverer).
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
    fun consumeRetry(
        payload: String,
        @Header(name = KafkaRetryHeaders.RETRY_NOT_BEFORE_MS, required = false) notBeforeRaw: String? = null,
    ) {
        awaitRetryWindow(notBeforeRaw)
        processPayload(payload)
    }

    private fun awaitRetryWindow(notBeforeRaw: String?) {
        val notBefore = notBeforeRaw?.toLongOrNull() ?: return
        val waitMs = notBefore - System.currentTimeMillis()
        if (waitMs <= 0L) {
            return
        }
        logger.info("event=transaction_retry_wait waitMs={}", waitMs)
        try {
            Thread.sleep(waitMs)
        } catch (ex: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IllegalStateException("Interrupted while waiting retry backoff", ex)
        }
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
            ProcessTransactionResult.IgnoredIneligible -> {
                balanceMetrics.incrementTransactionIgnoredIneligible()
                logger.info(
                    "event=transaction_processed accountId={} transactionId={} result=ignored_ineligible",
                    event.accountId,
                    event.transactionId,
                )
            }
            ProcessTransactionResult.IgnoredNotNewer -> {
                balanceMetrics.incrementTransactionIgnoredNotNewer()
                logger.info(
                    "event=transaction_processed accountId={} transactionId={} result=ignored_not_newer",
                    event.accountId,
                    event.transactionId,
                )
            }
        }
    }
}
