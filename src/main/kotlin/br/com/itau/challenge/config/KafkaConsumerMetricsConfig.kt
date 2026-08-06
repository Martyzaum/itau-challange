package br.com.itau.challenge.config

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.binder.MeterBinder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.KafkaListenerEndpointRegistry

/**
 * Exposes listener-container count for ops dashboards.
 * Fine-grained records-lag is available via Kafka client metrics when Micrometer Kafka binders are active.
 */
@Configuration
class KafkaConsumerMetricsConfig {

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
