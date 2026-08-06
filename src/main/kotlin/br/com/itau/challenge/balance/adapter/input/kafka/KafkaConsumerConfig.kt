package br.com.itau.challenge.balance.adapter.input.kafka

import br.com.itau.challenge.balance.domain.exception.InvalidBalanceException
import br.com.itau.challenge.balance.domain.exception.InvalidTransactionEventException
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
        @Value("\${transactions.async-retry.topics}") retryTopicsCsv: String,
    ): Array<String> = parseCsv(retryTopicsCsv).toTypedArray()

    @Bean
    fun kafkaErrorHandler(
        kafkaOperations: KafkaOperations<String, String>,
        @Value("\${transactions.dlt-topic-name}") dltTopicName: String,
        transactionRetryTopics: Array<String>,
    ): DefaultErrorHandler =
        createKafkaErrorHandler(
            kafkaOperations = kafkaOperations,
            dltTopicName = dltTopicName,
            retryTopics = transactionRetryTopics.toList(),
        )
}

internal fun parseCsv(value: String): List<String> =
    value
        .split(",")
        .map { it.trim() }
        .filter { it.isNotEmpty() }

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
        val future = kafkaOperations.send(outbound)
        future.get(10, TimeUnit.SECONDS)
    }

internal fun createKafkaErrorHandler(
    kafkaOperations: KafkaOperations<String, String>,
    dltTopicName: String,
    retryTopics: List<String>,
): DefaultErrorHandler {
    val recoverer =
        createAsyncRetryRecoverer(
            kafkaOperations = kafkaOperations,
            retryTopics = retryTopics,
            dltTopicName = dltTopicName,
        )
    return DefaultErrorHandler(recoverer, FixedBackOff(0L, 0L)).apply {
        addNotRetryableExceptions(*notRetryableExceptionTypes())
    }
}
