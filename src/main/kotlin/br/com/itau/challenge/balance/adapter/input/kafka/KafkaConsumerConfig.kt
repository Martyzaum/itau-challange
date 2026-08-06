package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
import br.com.itau.challenge.config.CircuitBreakerNames
import br.com.itau.challenge.config.executeAndTranslateOpen
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.listener.ConsumerRecordRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.serializer.DeserializationException
import org.springframework.util.backoff.FixedBackOff
import tools.jackson.core.JacksonException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

@Configuration
class KafkaConsumerConfig {

    @Bean
    fun transactionRetryTopics(
        @Value("\${transactions.topic-name}") topicName: String,
        @Value("\${transactions.retry.max-attempts}") maxAttempts: Int,
    ): Array<String> = buildRetryTopicNames(topicName, maxAttempts).toTypedArray()

    @Bean
    fun transactionRetryDelaysMs(
        @Value("\${transactions.retry.initial-interval-ms}") initialIntervalMs: Long,
        @Value("\${transactions.retry.multiplier}") multiplier: Double,
        @Value("\${transactions.retry.max-interval-ms}") maxIntervalMs: Long,
        @Value("\${transactions.retry.max-attempts}") maxAttempts: Int,
    ): List<Long> =
        buildRetryDelaysMs(
            initialIntervalMs = initialIntervalMs,
            multiplier = multiplier,
            maxIntervalMs = maxIntervalMs,
            maxAttempts = maxAttempts,
        )

    @Bean
    fun kafkaErrorHandler(
        kafkaOperations: KafkaOperations<String, String>,
        @Value("\${transactions.dlt-topic-name}") dltTopicName: String,
        transactionRetryTopics: Array<String>,
        circuitBreakerRegistry: CircuitBreakerRegistry,
    ): DefaultErrorHandler =
        createKafkaErrorHandler(
            kafkaOperations = kafkaOperations,
            dltTopicName = dltTopicName,
            retryTopics = transactionRetryTopics.toList(),
            produceCircuitBreaker = circuitBreakerRegistry.circuitBreaker(CircuitBreakerNames.KAFKA_PRODUCE),
        )
}

internal fun buildRetryTopicNames(
    topicName: String,
    maxAttempts: Int,
): List<String> {
    require(maxAttempts >= 1) { "transactions.retry.max-attempts must be >= 1" }
    return (1..maxAttempts).map { attempt -> "$topicName.retry-$attempt" }
}

internal fun buildRetryDelaysMs(
    initialIntervalMs: Long,
    multiplier: Double,
    maxIntervalMs: Long,
    maxAttempts: Int,
): List<Long> {
    require(maxAttempts >= 1) { "transactions.retry.max-attempts must be >= 1" }
    require(initialIntervalMs >= 0) { "transactions.retry.initial-interval-ms must be >= 0" }
    require(multiplier >= 1.0) { "transactions.retry.multiplier must be >= 1.0" }
    require(maxIntervalMs >= initialIntervalMs) {
        "transactions.retry.max-interval-ms must be >= initial-interval-ms"
    }
    var current = initialIntervalMs.toDouble()
    return (1..maxAttempts).map {
        val delay = current.toLong().coerceIn(0L, maxIntervalMs)
        current *= multiplier
        delay
    }
}

internal fun notRetryableExceptionTypes(): Array<Class<out Exception>> =
    arrayOf(
        JacksonException::class.java,
        IllegalArgumentException::class.java,
        InvalidTransactionEventException::class.java,
        InvalidBalanceException::class.java,
        DeserializationException::class.java,
    )

internal fun isNotRetryable(error: Throwable?): Boolean {
    var current: Throwable? = error
    val notRetryable = notRetryableExceptionTypes().toSet()
    while (current != null) {
        if (notRetryable.any { type -> type.isInstance(current) }) {
            return true
        }
        current = current.cause
    }
    return false
}

internal fun rootCause(error: Exception?): Throwable {
    var current: Throwable = error ?: RuntimeException("unknown")
    while (current.cause != null && current.cause !== current) {
        current = current.cause!!
    }
    return current
}

internal fun readRetryAttempt(record: ConsumerRecord<*, *>): Int {
    val header = record.headers().lastHeader(KafkaRetryHeaders.RETRY_ATTEMPT) ?: return 0
    return String(header.value(), StandardCharsets.UTF_8).toIntOrNull() ?: 0
}

internal fun resolveFailureDestination(
    record: ConsumerRecord<*, *>,
    exception: Exception?,
    retryTopics: List<String>,
    dltTopicName: String,
): TopicPartition {
    val partition = record.partition()
    if (isNotRetryable(exception) || retryTopics.isEmpty()) {
        return TopicPartition(dltTopicName, partition)
    }
    val nextAttempt = readRetryAttempt(record) + 1
    if (nextAttempt > retryTopics.size) {
        return TopicPartition(dltTopicName, partition)
    }
    return TopicPartition(retryTopics[nextAttempt - 1], partition)
}

internal fun createAsyncRetryRecoverer(
    kafkaOperations: KafkaOperations<String, String>,
    retryTopics: List<String>,
    dltTopicName: String,
    produceCircuitBreaker: CircuitBreaker,
): ConsumerRecordRecoverer =
    ConsumerRecordRecoverer { record, exception ->
        val destination =
            resolveFailureDestination(
                record = record,
                exception = exception,
                retryTopics = retryTopics,
                dltTopicName = dltTopicName,
            )
        val nextAttempt = readRetryAttempt(record) + 1
        val headers = RecordHeaders()
        record.headers().forEach { headers.add(it) }
        headers.remove(KafkaRetryHeaders.RETRY_ATTEMPT)
        headers.remove(KafkaRetryHeaders.RETRY_FAILED_AT_MS)
        headers.add(
            RecordHeader(
                KafkaRetryHeaders.RETRY_ATTEMPT,
                nextAttempt.toString().toByteArray(StandardCharsets.UTF_8),
            ),
        )
        headers.add(
            RecordHeader(
                KafkaRetryHeaders.RETRY_FAILED_AT_MS,
                System.currentTimeMillis().toString().toByteArray(StandardCharsets.UTF_8),
            ),
        )
        if (headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC) == null) {
            headers.add(
                RecordHeader(
                    KafkaHeaders.DLT_ORIGINAL_TOPIC,
                    record.topic().toByteArray(StandardCharsets.UTF_8),
                ),
            )
        }
        if (headers.lastHeader(KafkaRetryHeaders.ORIGINAL_TOPIC) == null) {
            headers.add(
                RecordHeader(
                    KafkaRetryHeaders.ORIGINAL_TOPIC,
                    record.topic().toByteArray(StandardCharsets.UTF_8),
                ),
            )
        }
        val root = rootCause(exception)
        headers.add(
            RecordHeader(
                KafkaHeaders.DLT_EXCEPTION_FQCN,
                root.javaClass.name.toByteArray(StandardCharsets.UTF_8),
            ),
        )
        root.message?.let { message ->
            headers.add(
                RecordHeader(
                    KafkaHeaders.DLT_EXCEPTION_MESSAGE,
                    message.toByteArray(StandardCharsets.UTF_8),
                ),
            )
        }
        val key = record.key()?.toString() ?: ""
        val value = record.value()?.toString() ?: ""
        val outbound =
            ProducerRecord(
                destination.topic(),
                destination.partition(),
                key,
                value,
                headers,
            )
        produceCircuitBreaker.executeAndTranslateOpen(CircuitBreakerNames.KAFKA_PRODUCE) {
            val future = kafkaOperations.send(outbound)
            future.get(10, TimeUnit.SECONDS)
        }
    }

internal fun createKafkaErrorHandler(
    kafkaOperations: KafkaOperations<String, String>,
    dltTopicName: String,
    retryTopics: List<String>,
    produceCircuitBreaker: CircuitBreaker,
): DefaultErrorHandler {
    val recoverer =
        createAsyncRetryRecoverer(
            kafkaOperations = kafkaOperations,
            retryTopics = retryTopics,
            dltTopicName = dltTopicName,
            produceCircuitBreaker = produceCircuitBreaker,
        )
    return DefaultErrorHandler(recoverer, FixedBackOff(0L, 0L)).apply {
        addNotRetryableExceptions(*notRetryableExceptionTypes())
    }
}
