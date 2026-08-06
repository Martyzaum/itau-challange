package br.com.itau.challenge.balance.adapter.input.kafka

import java.util.concurrent.ThreadLocalRandom
import kotlin.math.pow

/**
 * Exponential backoff with full jitter for async Kafka retry hops.
 * Delay is applied when the message is *consumed* from retry-N (not via Thread.sleep
 * on the main topic consumer / recoverer path).
 */
object RetryBackoff {
    fun delayMs(
        attempt: Int,
        initialIntervalMs: Long,
        multiplier: Double,
        maxIntervalMs: Long,
    ): Long {
        require(attempt >= 1) { "attempt must be >= 1" }
        require(initialIntervalMs >= 0) { "initialIntervalMs must be >= 0" }
        require(multiplier >= 1.0) { "multiplier must be >= 1.0" }
        require(maxIntervalMs >= initialIntervalMs) { "maxIntervalMs must be >= initialIntervalMs" }

        val exp =
            (initialIntervalMs * multiplier.pow((attempt - 1).toDouble()))
                .toLong()
                .coerceAtMost(maxIntervalMs)
        if (exp <= 0L) {
            return 0L
        }
        return ThreadLocalRandom.current().nextLong(0L, exp + 1L)
    }

    fun notBeforeMs(
        nowMs: Long,
        attempt: Int,
        initialIntervalMs: Long,
        multiplier: Double,
        maxIntervalMs: Long,
    ): Long = nowMs + delayMs(attempt, initialIntervalMs, multiplier, maxIntervalMs)
}
