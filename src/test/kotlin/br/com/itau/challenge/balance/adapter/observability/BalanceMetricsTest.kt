package br.com.itau.challenge.balance.adapter.observability

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.test.Test
import kotlin.test.assertEquals

class BalanceMetricsTest {

    @Test
    fun `should increment transaction and query counters with result tags`() {
        val registry = SimpleMeterRegistry()
        val metrics = BalanceMetrics(registry)

        metrics.incrementTransactionSaved()
        metrics.incrementTransactionIgnored()
        metrics.incrementBalanceFound()
        metrics.incrementBalanceNotFound()

        assertEquals(1.0, registry.counter("balance.transactions", "result", "saved").count())
        assertEquals(1.0, registry.counter("balance.transactions", "result", "ignored").count())
        assertEquals(1.0, registry.counter("balance.queries", "result", "found").count())
        assertEquals(1.0, registry.counter("balance.queries", "result", "not_found").count())
    }
}
