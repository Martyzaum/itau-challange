package br.com.itau.challenge.balance.adapter.input.web.security

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.core.io.DefaultResourceLoader
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import tools.jackson.module.kotlin.readValue

@ConfigurationProperties(prefix = "api")
data class ApiSecurityProperties(
    val auth: Auth = Auth(),
    val rateLimit: RateLimit = RateLimit(),
) {
    data class Auth(
        val enabled: Boolean = true,
        val header: String = "X-API-Key",
        /** Comma-separated keys (env). */
        val keys: String = "",
        /**
         * Optional JSON resource, e.g. `classpath:api-keys.json` with `{"keys":["k1","k2"]}`.
         * Merged with [keys].
         */
        val keysFile: String = "classpath:api-keys.json",
    ) {
        private val cachedKeys: List<String> by lazy(LazyThreadSafetyMode.PUBLICATION) {
            val fromCsv =
                keys
                    .split(',')
                    .map { it.trim() }
                    .filter { it.isNotEmpty() }
            val fromFile = loadKeysFromFile(keysFile)
            (fromCsv + fromFile).distinct()
        }

        fun parsedKeys(): List<String> = cachedKeys

        private fun loadKeysFromFile(location: String): List<String> {
            if (location.isBlank()) {
                return emptyList()
            }
            return try {
                val resource = DefaultResourceLoader().getResource(location)
                if (!resource.exists()) {
                    return emptyList()
                }
                val mapper = JsonMapper.builder().addModule(kotlinModule()).build()
                val payload = mapper.readValue<ApiKeysFile>(resource.inputStream)
                payload.keys.map { it.trim() }.filter { it.isNotEmpty() }
            } catch (_: Exception) {
                emptyList()
            }
        }
    }

    data class RateLimit(
        val enabled: Boolean = false,
        val requestsPerMinute: Int = 120,
    )

    data class ApiKeysFile(
        val keys: List<String> = emptyList(),
    )
}
