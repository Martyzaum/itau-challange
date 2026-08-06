package br.com.itau.challenge.balance.adapter.input.web.security

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ApiSecuritySupportTest {

    @Test
    fun `should treat actuator and openapi as public`() {
        assertTrue(ApiSecurityPaths.isPublic("/actuator/health"))
        assertTrue(ApiSecurityPaths.isPublic("/actuator/health/liveness"))
        assertTrue(ApiSecurityPaths.isPublic("/openapi.yaml"))
        assertFalse(ApiSecurityPaths.isPublic("/balances/abc"))
    }

    @Test
    fun `should compare api keys in constant time`() {
        assertTrue(constantTimeEquals("secret", "secret"))
        assertFalse(constantTimeEquals("secret", "Secret"))
        assertFalse(constantTimeEquals("short", "longer-key"))
    }

    @Test
    fun `should parse comma separated keys`() {
        val props =
            ApiSecurityProperties(
                auth = ApiSecurityProperties.Auth(keys = " a ,b, ,c ", keysFile = ""),
            )
        assertEquals(listOf("a", "b", "c"), props.auth.parsedKeys())
    }

    @Test
    fun `should merge csv keys with classpath api-keys json`() {
        val props =
            ApiSecurityProperties(
                auth =
                    ApiSecurityProperties.Auth(
                        keys = "extra-key",
                        keysFile = "classpath:api-keys.json",
                    ),
            )
        val keys = props.auth.parsedKeys()
        assertTrue(keys.contains("extra-key"))
        assertTrue(keys.contains("local-dev-key"))
        assertTrue(keys.contains("reviewer-key"))
    }
}
