package br.com.itau.challenge.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI
import java.time.Duration

@Configuration
class DynamoDbConfig {

    @Bean
    fun dynamoDbClient(
        @Value("\${dynamodb.endpoint:}") endpoint: String,
        @Value("\${dynamodb.region}") region: String,
        @Value("\${dynamodb.api-call-timeout-ms:5000}") apiCallTimeoutMs: Long,
        @Value("\${dynamodb.api-call-attempt-timeout-ms:3000}") apiCallAttemptTimeoutMs: Long,
    ): DynamoDbClient {
        val settings =
            resolveDynamoDbConnectionSettings(
                endpoint = endpoint,
                region = region,
                apiCallTimeoutMs = apiCallTimeoutMs,
                apiCallAttemptTimeoutMs = apiCallAttemptTimeoutMs,
            )
        val builder =
            DynamoDbClient
                .builder()
                .region(Region.of(settings.region))
                .overrideConfiguration(
                    ClientOverrideConfiguration
                        .builder()
                        .apiCallTimeout(Duration.ofMillis(settings.apiCallTimeoutMs))
                        .apiCallAttemptTimeout(Duration.ofMillis(settings.apiCallAttemptTimeoutMs))
                        .build(),
                )

        if (settings.endpointOverride != null) {
            builder
                .endpointOverride(URI.create(settings.endpointOverride))
                .credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")),
                )
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.builder().build())
        }

        return builder.build()
    }
}

internal data class DynamoDbConnectionSettings(
    val region: String,
    val endpointOverride: String?,
    val apiCallTimeoutMs: Long,
    val apiCallAttemptTimeoutMs: Long,
)

internal fun resolveDynamoDbConnectionSettings(
    endpoint: String?,
    region: String,
    apiCallTimeoutMs: Long = 5_000L,
    apiCallAttemptTimeoutMs: Long = 3_000L,
): DynamoDbConnectionSettings {
    val trimmedEndpoint = endpoint?.trim().orEmpty()
    return DynamoDbConnectionSettings(
        region = region,
        endpointOverride = trimmedEndpoint.ifEmpty { null },
        apiCallTimeoutMs = apiCallTimeoutMs,
        apiCallAttemptTimeoutMs = apiCallAttemptTimeoutMs,
    )
}
