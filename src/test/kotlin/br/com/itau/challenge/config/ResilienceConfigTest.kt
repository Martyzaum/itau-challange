package br.com.itau.challenge.config

import br.com.itau.challenge.balance.domain.exception.DependencyUnavailableException
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ResilienceConfigTest {

    @Test
    fun `should register named circuit breakers and bind metrics`() {
        val meterRegistry = SimpleMeterRegistry()
        val registry =
            ResilienceConfig().circuitBreakerRegistry(
                meterRegistry = meterRegistry,
                failureRateThreshold = 50f,
                slidingWindowSize = 20,
                minimumNumberOfCalls = 10,
                waitDurationInOpenStateMs = 30_000L,
                permittedNumberOfCallsInHalfOpenState = 5,
            )

        CircuitBreakerNames.ALL.forEach { name ->
            assertEquals(name, registry.circuitBreaker(name).name)
        }
        assertTrue(meterRegistry.meters.any { it.id.name.startsWith("resilience4j.circuitbreaker") })
    }

    @Test
    fun `should translate open circuit into dependency unavailable`() {
        val registry = CircuitBreakerRegistry.ofDefaults()
        val breaker = registry.circuitBreaker(CircuitBreakerNames.DYNAMODB)
        breaker.transitionToOpenState()

        val error =
            assertFailsWith<DependencyUnavailableException> {
                breaker.executeAndTranslateOpen(CircuitBreakerNames.DYNAMODB) { "ok" }
            }
        assertEquals(CircuitBreakerNames.DYNAMODB, error.dependency)
    }

    @Test
    fun `should execute supplier when circuit is closed`() {
        val registry = CircuitBreakerRegistry.ofDefaults()
        val breaker = registry.circuitBreaker(CircuitBreakerNames.DYNAMODB)

        val value = breaker.executeAndTranslateOpen(CircuitBreakerNames.DYNAMODB) { 42 }
        assertEquals(42, value)
    }
}
