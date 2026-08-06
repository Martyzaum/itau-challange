package br.com.itau.challenge.config

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import jakarta.annotation.PostConstruct
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.MicrometerConsumerListener

/**
 * Kafka consumer metrics for ops/SigNoz:
 * - MicrometerConsumerListener → client metrics including **records-lag** per topic/partition
 * - listener container count gauge
 */
@Configuration
class KafkaConsumerMetricsConfig(
    private val consumerFactory: ConsumerFactory<*, *>,
    private val meterRegistry: MeterRegistry,
) {

    @PostConstruct
    fun bindConsumerClientMetrics() {
        val factory = consumerFactory
        if (factory is DefaultKafkaConsumerFactory<*, *>) {
            @Suppress("UNCHECKED_CAST")
            val typed = factory as DefaultKafkaConsumerFactory<Any, Any>
            val alreadyBound =
                typed.getListeners().any { listener ->
                    listener is MicrometerConsumerListener<*, *>
                }
            if (!alreadyBound) {
                typed.addListener(MicrometerConsumerListener(meterRegistry))
            }
        }
    }

    @Bean
    fun kafkaListenerContainerCountBinder(registry: KafkaListenerEndpointRegistry): MeterBinder =
        MeterBinder { meters ->
            Gauge
                .builder("kafka.consumer.listener.containers") {
                    registry.allListenerContainers.size.toDouble()
                }.description("Number of Spring Kafka listener containers")
                .register(meters)
        }
}
