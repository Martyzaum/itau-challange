package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.adapter.input.kafka.dto.FinancialTransactionMessage
import br.com.itau.challenge.balance.port.input.ProcessTransactionEventUseCase
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class TransactionEventConsumer(
    private val processTransactionEventUseCase: ProcessTransactionEventUseCase,
    private val objectMapper: ObjectMapper,
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["\${transactions.topic-name}"])
    fun consume(payload: String) {
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
    }
}
