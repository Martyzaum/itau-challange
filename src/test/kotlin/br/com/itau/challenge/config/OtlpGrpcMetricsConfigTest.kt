package br.com.itau.challenge.config

import io.micrometer.core.instrument.MeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest
@TestPropertySource(
    properties = [
        "management.otlp.metrics.export.enabled=true",
        "management.otlp.metrics.export.step=1h",
        "management.opentelemetry.metrics.export.otlp.endpoint=http://127.0.0.1:4317",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
    ],
)
class OtlpGrpcMetricsConfigTest {
    @Autowired
    private lateinit var meterRegistries: List<MeterRegistry>

    @Test
    fun `should register open telemetry meter registry when metrics export is enabled`() {
        assertTrue(
            meterRegistries.any {
                it.javaClass.name.contains("OpenTelemetryMeterRegistry")
            },
        )
        assertNotNull(meterRegistries.first())
    }
}
