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
        val props = ApiSecurityProperties(auth = ApiSecurityProperties.Auth(keys = " a ,b, ,c "))
        assertEquals(listOf("a", "b", "c"), props.auth.parsedKeys())
    }
}
