package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.adapter.observability.BalanceMetrics
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.nio.charset.StandardCharsets

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
    private val transactionRetryDelaysMs: List<Long>,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${transactions.topic-name}"])
    fun consume(payload: String) {
        processPayload(payload)
    }

    @KafkaListener(topics = ["#{@transactionRetryTopics}"])
    fun consumeRetry(record: ConsumerRecord<String, String>) {
        awaitRetryDelay(record)
        processPayload(record.value())
    }

    private fun processPayload(payload: String) {
        val message = objectMapper.readValue(payload, FinancialTransactionMessage::class.java)
        val event = message.toDomain()
        val saved = processTransactionEventUseCase.processTransactionEvent(event)
        if (saved) {
            balanceMetrics.incrementTransactionSaved()
            logger.info(
                "event=transaction_processed accountId={} transactionId={} result=saved",
                event.accountId,
                event.transactionId,
            )
        } else {
            balanceMetrics.incrementTransactionIgnored()
            logger.info(
                "event=transaction_processed accountId={} transactionId={} result=ignored",
                event.accountId,
                event.transactionId,
            )
        }
    }

    private fun awaitRetryDelay(record: ConsumerRecord<String, String>) {
        if (transactionRetryDelaysMs.isEmpty()) return
        val attempt = readRetryAttempt(record).coerceAtLeast(1)
        val delayMs = transactionRetryDelaysMs[minOf(attempt, transactionRetryDelaysMs.size) - 1]
        val failedAtHeader = record.headers().lastHeader(KafkaRetryHeaders.RETRY_FAILED_AT_MS)
        val failedAtMs =
            failedAtHeader
                ?.let { String(it.value(), StandardCharsets.UTF_8).toLongOrNull() }
                ?: System.currentTimeMillis()
        val waitMs = failedAtMs + delayMs - System.currentTimeMillis()
        if (waitMs > 0) {
            Thread.sleep(waitMs.coerceAtMost(MAX_SLEEP_MS))
        }
    }

    private companion object {
        const val MAX_SLEEP_MS = 60_000L
    }
}
