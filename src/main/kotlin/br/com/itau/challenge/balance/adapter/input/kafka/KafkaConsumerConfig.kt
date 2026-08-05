package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries
import org.springframework.util.backoff.BackOff
import tools.jackson.core.JacksonException

@Configuration
class KafkaConsumerConfig {

    @Bean
    fun kafkaErrorHandler(
        kafkaOperations: KafkaOperations<String, String>,
        @Value("\${transactions.dlt-topic-name}") dltTopicName: String,
        @Value("\${transactions.retry.initial-interval-ms}") initialIntervalMs: Long,
        @Value("\${transactions.retry.multiplier}") multiplier: Double,
        @Value("\${transactions.retry.max-interval-ms}") maxIntervalMs: Long,
        @Value("\${transactions.retry.max-attempts}") maxAttempts: Int,
    ): DefaultErrorHandler =
        createKafkaErrorHandler(
            kafkaOperations = kafkaOperations,
            dltTopicName = dltTopicName,
            backOff =
                createRetryBackOff(
                    initialIntervalMs = initialIntervalMs,
                    multiplier = multiplier,
                    maxIntervalMs = maxIntervalMs,
                    maxAttempts = maxAttempts,
                ),
        )
}

internal fun createRetryBackOff(
    initialIntervalMs: Long,
    multiplier: Double,
    maxIntervalMs: Long,
    maxAttempts: Int,
): ExponentialBackOffWithMaxRetries =
    ExponentialBackOffWithMaxRetries(maxAttempts).apply {
        initialInterval = initialIntervalMs
        this.multiplier = multiplier
        maxInterval = maxIntervalMs
    }

internal fun notRetryableExceptionTypes(): Array<Class<out Exception>> =
    arrayOf(
        JacksonException::class.java,
        IllegalArgumentException::class.java,
        InvalidTransactionEventException::class.java,
        InvalidBalanceException::class.java,
    )

internal fun dltTopicPartition(
    dltTopicName: String,
    record: ConsumerRecord<*, *>,
): TopicPartition = TopicPartition(dltTopicName, record.partition())

internal fun createKafkaErrorHandler(
    kafkaOperations: KafkaOperations<String, String>,
    dltTopicName: String,
    backOff: BackOff,
): DefaultErrorHandler {
    val recoverer =
        DeadLetterPublishingRecoverer(kafkaOperations) { record, _ ->
            dltTopicPartition(dltTopicName, record)
        }

    return DefaultErrorHandler(recoverer, backOff).apply {
        addNotRetryableExceptions(*notRetryableExceptionTypes())
    }
}
