package br.com.itau.challenge.config

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.logs.Severity
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConditionalOnProperty(
    prefix = "management.logging.export.otlp",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = false,
)
class OpenTelemetryLogbackConfig {

    @Bean
    fun openTelemetryLogbackAppenderInstaller(
        openTelemetry: OpenTelemetry,
    ): ApplicationRunner =
        ApplicationRunner { _: ApplicationArguments ->
            val loggerContext = LoggerFactory.getILoggerFactory() as LoggerContext
            val root = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME)

            val existing =
                root
                    .iteratorForAppenders()
                    .asSequence()
                    .filterIsInstance<OpenTelemetryAppender>()
                    .toList()

            if (existing.isEmpty()) {
                val appender =
                    OpenTelemetryAppender().apply {
                        context = loggerContext
                        name = "OTEL"
                        setCaptureExperimentalAttributes(true)
                        setCaptureCodeAttributes(true)
                        setCaptureMarkerAttribute(true)
                        setCaptureKeyValuePairAttributes(true)
                        setCaptureLoggerContext(true)
                        setCaptureMdcAttributes("*")
                        start()
                    }
                root.addAppender(appender)
            }

            OpenTelemetryAppender.install(openTelemetry)

            openTelemetry.logsBridge
                .get("br.com.itau.challenge.config.OpenTelemetryLogbackConfig")
                .logRecordBuilder()
                .setSeverity(Severity.INFO)
                .setBody("otel_logs_pipeline_ready")
                .emit()

            LoggerFactory
                .getLogger("br.com.itau.challenge.config.OpenTelemetryLogbackConfig")
                .info("event=otel_logback_appender_installed")
        }
}
