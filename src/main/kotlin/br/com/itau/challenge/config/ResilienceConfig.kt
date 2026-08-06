package br.com.itau.challenge.config

import br.com.itau.challenge.balance.domain.exception.DependencyUnavailableException
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.core.exception.SdkException
import java.time.Duration

@Configuration
class ResilienceConfig {

    @Bean
    fun circuitBreakerRegistry(
        meterRegistry: MeterRegistry,
        @Value("\${resilience.circuitbreaker.failure-rate-threshold:50}") failureRateThreshold: Float,
        @Value("\${resilience.circuitbreaker.sliding-window-size:20}") slidingWindowSize: Int,
        @Value("\${resilience.circuitbreaker.minimum-number-of-calls:10}") minimumNumberOfCalls: Int,
        @Value("\${resilience.circuitbreaker.wait-duration-in-open-state-ms:30000}") waitDurationInOpenStateMs: Long,
        @Value("\${resilience.circuitbreaker.permitted-number-of-calls-in-half-open-state:5}")
        permittedNumberOfCallsInHalfOpenState: Int,
    ): CircuitBreakerRegistry {
        val defaultConfig =
            CircuitBreakerConfig
                .custom()
                .failureRateThreshold(failureRateThreshold)
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumNumberOfCalls)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenStateMs))
                .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
                .build()

        val registry = CircuitBreakerRegistry.of(defaultConfig)
        CircuitBreakerNames.ALL.forEach { name -> registry.circuitBreaker(name) }
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry)
        return registry
    }
}

object CircuitBreakerNames {
    const val DYNAMODB_READ = "dynamodb-read"
    const val DYNAMODB_WRITE = "dynamodb-write"
    const val REDIS = "redis"
    const val KAFKA_PRODUCE = "kafka-produce"

    val ALL = listOf(DYNAMODB_READ, DYNAMODB_WRITE, REDIS, KAFKA_PRODUCE)
}

fun <T> CircuitBreaker.executeAndTranslateOpen(
    dependency: String,
    block: () -> T,
): T =
    try {
        this.executeSupplier {
            try {
                block()
            } catch (ex: SdkException) {
                throw DependencyUnavailableException(dependency, ex)
            }
        }
    } catch (ex: CallNotPermittedException) {
        throw DependencyUnavailableException(dependency, ex)
    } catch (ex: DependencyUnavailableException) {
        throw ex
    }
