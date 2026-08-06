package br.com.itau.challenge.config

import io.opentelemetry.sdk.logs.export.LogRecordExporter
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertTrue

@SpringBootTest
@TestPropertySource(
    properties = [
        "management.otlp.metrics.export.enabled=false",
        "management.tracing.enabled=false",
        "management.logging.export.otlp.enabled=true",
        "management.opentelemetry.logging.export.otlp.endpoint=http://127.0.0.1:4317",
        "management.opentelemetry.logging.export.otlp.transport=grpc",
        "spring.kafka.listener.auto-startup=false",
    ],
)
class OtlpLoggingConfigTest {
    @Autowired
    private lateinit var context: ApplicationContext

    @Test
    fun `should create otlp log record exporter when logging export enabled`() {
        val exporters = context.getBeansOfType(LogRecordExporter::class.java)
        assertTrue(exporters.isNotEmpty(), "expected LogRecordExporter beans, got ${exporters.keys}")
    }
}
