package br.com.itau.challenge.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

@Configuration
class DynamoDbConfig {

    @Bean
    fun dynamoDbClient(
        @Value("\${dynamodb.endpoint:}") endpoint: String,
        @Value("\${dynamodb.region}") region: String,
    ): DynamoDbClient {
        val settings = resolveDynamoDbConnectionSettings(endpoint = endpoint, region = region)
        val builder = DynamoDbClient.builder().region(Region.of(settings.region))

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
)

internal fun resolveDynamoDbConnectionSettings(
    endpoint: String?,
    region: String,
): DynamoDbConnectionSettings {
    val trimmedEndpoint = endpoint?.trim().orEmpty()
    return if (trimmedEndpoint.isEmpty()) {
        DynamoDbConnectionSettings(region = region, endpointOverride = null)
    } else {
        DynamoDbConnectionSettings(region = region, endpointOverride = trimmedEndpoint)
    }
}
