package br.com.itau.challenge.config

import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import tools.jackson.databind.ObjectMapper
import java.time.Duration

@Configuration
@ConditionalOnProperty(prefix = "balance.cache", name = ["enabled"], havingValue = "true")
class RedisBalanceCacheConfig {
    @Bean(destroyMethod = "shutdown")
    fun balanceRedisClient(
        @Value("\${balance.cache.redis.host}") host: String,
        @Value("\${balance.cache.redis.port}") port: Int,
        @Value("\${balance.cache.redis.timeout-ms}") timeoutMs: Long,
    ): RedisClient {
        val uri =
            RedisURI.Builder
                .redis(host, port)
                .withTimeout(Duration.ofMillis(timeoutMs))
                .build()
        return RedisClient.create(uri)
    }

    @Bean(destroyMethod = "close")
    fun balanceRedisConnection(balanceRedisClient: RedisClient): StatefulRedisConnection<String, String> =
        balanceRedisClient.connect()

    @Bean
    fun redisAccountBalanceCache(
        balanceRedisConnection: StatefulRedisConnection<String, String>,
        objectMapper: ObjectMapper,
        @Value("\${balance.cache.key-prefix}") keyPrefix: String,
        @Value("\${balance.cache.ttl-seconds}") ttlSeconds: Long,
    ): RedisAccountBalanceCache {
        val ttl = if (ttlSeconds > 0) Duration.ofSeconds(ttlSeconds) else null
        return RedisAccountBalanceCache(
            commands = balanceRedisConnection.sync(),
            objectMapper = objectMapper,
            keyPrefix = keyPrefix,
            ttl = ttl,
        )
    }
}
