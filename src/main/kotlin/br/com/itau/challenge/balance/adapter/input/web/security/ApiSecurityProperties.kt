package br.com.itau.challenge.balance.adapter.input.web.security

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "api")
data class ApiSecurityProperties(
    val auth: Auth = Auth(),
    val rateLimit: RateLimit = RateLimit(),
) {
    data class Auth(
        val enabled: Boolean = false,
        val header: String = "X-API-Key",
        val keys: String = "",
    ) {
        fun parsedKeys(): List<String> =
            keys
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
    }

    data class RateLimit(
        val enabled: Boolean = false,
        val requestsPerMinute: Int = 120,
    )
}
