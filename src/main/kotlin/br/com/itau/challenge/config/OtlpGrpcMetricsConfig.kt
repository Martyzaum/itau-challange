package br.com.itau.challenge.config

import io.micrometer.core.instrument.MeterRegistry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.instrumentation.micrometer.v1_5.OpenTelemetryMeterRegistry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
@ConditionalOnProperty(
    prefix = "management.otlp.metrics.export",
    name = ["enabled"],
    havingValue = "true",
)
class OtlpGrpcMetricsConfig {

    @Bean(destroyMethod = "shutdown")
    fun otlpGrpcMetricExporter(
        @Value("\${management.opentelemetry.metrics.export.otlp.endpoint:http://localhost:4317}")
        endpoint: String,
    ): OtlpGrpcMetricExporter =
        OtlpGrpcMetricExporter
            .builder()
            .setEndpoint(endpoint)
            .build()

    @Bean(destroyMethod = "close")
    fun sdkMeterProvider(
        otlpGrpcMetricExporter: OtlpGrpcMetricExporter,
        @Value("\${management.otlp.metrics.export.step:30s}") step: Duration,
        @Value("\${spring.application.name:itau-balance-api}") applicationName: String,
    ): SdkMeterProvider {
        val resource =
            Resource
                .getDefault()
                .merge(
                    Resource.create(
                        Attributes.of(AttributeKey.stringKey("service.name"), applicationName),
                    ),
                )

        return SdkMeterProvider
            .builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader
                    .builder(otlpGrpcMetricExporter)
                    .setInterval(step)
                    .build(),
            ).build()
    }

    @Bean(destroyMethod = "close")
    fun openTelemetryMeterRegistry(sdkMeterProvider: SdkMeterProvider): MeterRegistry {
        val openTelemetry =
            OpenTelemetrySdk
                .builder()
                .setMeterProvider(sdkMeterProvider)
                .build()

        return OpenTelemetryMeterRegistry
            .builder(openTelemetry)
            .build()
    }
}
