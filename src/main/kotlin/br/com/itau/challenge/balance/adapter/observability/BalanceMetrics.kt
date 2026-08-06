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

    private val transactionsIgnoredIneligible: Counter =
        Counter
            .builder(TRANSACTIONS_TOTAL)
            .tag(TAG_RESULT, RESULT_IGNORED_INELIGIBLE)
            .description("Events ignored: declined/rejected or account disabled")
            .register(meterRegistry)

    private val transactionsIgnoredNotNewer: Counter =
        Counter
            .builder(TRANSACTIONS_TOTAL)
            .tag(TAG_RESULT, RESULT_IGNORED_NOT_NEWER)
            .description("Events ignored: duplicate or stale version")
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

    private val transactionsRetried: Counter =
        Counter
            .builder(TRANSACTIONS_TOTAL)
            .tag(TAG_RESULT, RESULT_RETRIED)
            .description("Events routed to async retry topics")
            .register(meterRegistry)

    private val transactionsDlt: Counter =
        Counter
            .builder(TRANSACTIONS_TOTAL)
            .tag(TAG_RESULT, RESULT_DLT)
            .description("Events routed to DLT")
            .register(meterRegistry)

    private val cachePutFailed: Counter =
        Counter
            .builder(CACHE_TOTAL)
            .tag(TAG_RESULT, RESULT_PUT_FAILED)
            .description("Cache put failures after successful Dynamo save")
            .register(meterRegistry)

    fun incrementTransactionSaved() {
        transactionsSaved.increment()
    }

    fun incrementTransactionIgnoredIneligible() {
        transactionsIgnoredIneligible.increment()
    }

    fun incrementTransactionIgnoredNotNewer() {
        transactionsIgnoredNotNewer.increment()
    }

    fun incrementTransactionRetried() {
        transactionsRetried.increment()
    }

    fun incrementTransactionDlt() {
        transactionsDlt.increment()
    }

    fun incrementCachePutFailed() {
        cachePutFailed.increment()
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
        const val RESULT_IGNORED_INELIGIBLE = "ignored_ineligible"
        const val RESULT_IGNORED_NOT_NEWER = "ignored_not_newer"
        const val RESULT_FOUND = "found"
        const val RESULT_NOT_FOUND = "not_found"
        const val RESULT_HIT = "hit"
        const val RESULT_MISS = "miss"
        const val RESULT_RETRIED = "retried"
        const val RESULT_DLT = "dlt"
        const val RESULT_PUT_FAILED = "put_failed"
    }
}
