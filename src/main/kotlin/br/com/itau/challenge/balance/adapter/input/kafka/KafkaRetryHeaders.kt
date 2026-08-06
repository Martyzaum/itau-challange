package br.com.itau.challenge.balance.adapter.input.kafka

object KafkaRetryHeaders {
    const val RETRY_ATTEMPT = "x-retry-attempt"
    const val RETRY_FAILED_AT_MS = "x-retry-failed-at-ms"
    const val ORIGINAL_TOPIC = "x-original-topic"
}
