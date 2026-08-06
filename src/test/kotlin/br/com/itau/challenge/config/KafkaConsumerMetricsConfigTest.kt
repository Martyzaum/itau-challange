package br.com.itau.challenge.config

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.MicrometerConsumerListener
import kotlin.test.Test
import kotlin.test.assertTrue

class KafkaConsumerMetricsConfigTest {

    @Test
    fun `should register micrometer consumer listener for lag metrics`() {
        val factory =
            DefaultKafkaConsumerFactory<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "metrics-test",
                ),
            )
        val registry = SimpleMeterRegistry()
        val config = KafkaConsumerMetricsConfig(factory, registry)

        config.bindConsumerClientMetrics()

        assertTrue(
            factory.getListeners().any { it is MicrometerConsumerListener<*, *> },
            "MicrometerConsumerListener must be registered for per-partition lag gauges",
        )
    }
}
