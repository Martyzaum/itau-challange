package br.com.itau.challenge.balance.adapter.observability

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

/**
 * Business metrics registered in Micrometer.
 * Exported via OTLP (`micrometer-registry-otlp`) to any OpenTelemetry collector.
 */
@Component
class BalanceMetrics(
    meterRegistry: MeterRegistry,
) {
    private val transactionsSaved: Counter =
        Counter
            .builder(TRANSACTIONS_TOTAL)
            .tag(TAG_RESULT, RESULT_SAVED)
            .description("Financial transaction events that updated account balance")
            .register(meterRegistry)

    private val transactionsIgnored: Counter =
        Counter
            .builder(TRANSACTIONS_TOTAL)
            .tag(TAG_RESULT, RESULT_IGNORED)
            .description("Financial transaction events ignored (ineligible, duplicate or stale)")
            .register(meterRegistry)

    private val balanceFound: Counter =
        Counter
            .builder(QUERIES_TOTAL)
            .tag(TAG_RESULT, RESULT_FOUND)
            .description("Successful balance queries")
            .register(meterRegistry)

    private val balanceNotFound: Counter =
        Counter
            .builder(QUERIES_TOTAL)
            .tag(TAG_RESULT, RESULT_NOT_FOUND)
            .description("Balance queries for unknown accounts")
            .register(meterRegistry)

    private val cacheHit: Counter =
        Counter
            .builder(CACHE_TOTAL)
            .tag(TAG_RESULT, RESULT_HIT)
            .description("Balance cache hits")
            .register(meterRegistry)

    private val cacheMiss: Counter =
        Counter
            .builder(CACHE_TOTAL)
            .tag(TAG_RESULT, RESULT_MISS)
            .description("Balance cache misses")
            .register(meterRegistry)

    fun incrementTransactionSaved() {
        transactionsSaved.increment()
    }

    fun incrementTransactionIgnored() {
        transactionsIgnored.increment()
    }

    fun incrementBalanceFound() {
        balanceFound.increment()
    }

    fun incrementBalanceNotFound() {
        balanceNotFound.increment()
    }

    fun incrementCacheHit() {
        cacheHit.increment()
    }

    fun incrementCacheMiss() {
        cacheMiss.increment()
    }

    private companion object {
        const val TRANSACTIONS_TOTAL = "balance.transactions"
        const val QUERIES_TOTAL = "balance.queries"
        const val CACHE_TOTAL = "balance.cache"
        const val TAG_RESULT = "result"
        const val RESULT_SAVED = "saved"
        const val RESULT_IGNORED = "ignored"
        const val RESULT_FOUND = "found"
        const val RESULT_NOT_FOUND = "not_found"
        const val RESULT_HIT = "hit"
        const val RESULT_MISS = "miss"
    }
}
