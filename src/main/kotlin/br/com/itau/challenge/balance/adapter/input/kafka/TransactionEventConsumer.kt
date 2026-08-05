package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import tools.jackson.core.JacksonException

@Component
class TransactionEventConsumer(
    private val processTransactionEventUseCase: ProcessTransactionEventUseCase,
    private val objectMapper: ObjectMapper,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${transactions.topic-name}"])
    fun consume(payload: String) {
        try {
            val message = objectMapper.readValue(payload, FinancialTransactionMessage::class.java)
            val event = message.toDomain()
            val saved = processTransactionEventUseCase.processTransactionEvent(event)
            if (saved) {
                logger.info(
                    "Processed transaction event accountId={} transactionId={} saved=true",
                    event.accountId,
                    event.transactionId,
                )
            } else {
                logger.info(
                    "Ignored transaction event accountId={} transactionId={} saved=false",
                    event.accountId,
                    event.transactionId,
                )
            }
        } catch (exception: Exception) {
            if (exception.isDefinitivePayloadError()) {
                logger.warn("Discarding invalid transaction payload: {}", exception.message)
                return
            }
            throw exception
        }
    }
}

private fun Exception.isDefinitivePayloadError(): Boolean =
    this is JacksonException ||
        this is IllegalArgumentException ||
        this is InvalidTransactionEventException ||
        this is InvalidBalanceException
