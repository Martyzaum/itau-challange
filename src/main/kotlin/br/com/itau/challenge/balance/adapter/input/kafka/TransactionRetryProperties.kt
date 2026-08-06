package br.com.itau.challenge.balance.adapter.input.kafka

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "transactions.retry")
data class TransactionRetryProperties(
    val maxAttempts: Int = 3,
    /** Base delay before first retry hop is eligible for processing. */
    val initialIntervalMs: Long = 1_000,
    val multiplier: Double = 2.0,
    val maxIntervalMs: Long = 30_000,
)
