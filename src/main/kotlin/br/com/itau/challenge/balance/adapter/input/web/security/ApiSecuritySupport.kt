package br.com.itau.challenge.balance.adapter.input.web.security

import br.com.itau.challenge.balance.adapter.input.web.dto.ApiErrorResponse
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import tools.jackson.databind.ObjectMapper
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

internal object ApiSecurityPaths {
    fun isPublic(path: String): Boolean =
        path.startsWith("/actuator") ||
            path == "/openapi.yaml" ||
            path.startsWith("/error")
}

internal fun writeApiError(
    response: HttpServletResponse,
    objectMapper: ObjectMapper,
    status: Int,
    code: String,
    message: String,
) {
    response.status = status
    response.contentType = "application/json"
    response.characterEncoding = StandardCharsets.UTF_8.name()
    objectMapper.writeValue(
        response.outputStream,
        ApiErrorResponse(code = code, message = message),
    )
}

internal fun clientKey(
    request: HttpServletRequest,
    apiKeyHeader: String,
): String {
    val apiKey = request.getHeader(apiKeyHeader)?.trim().orEmpty()
    if (apiKey.isNotEmpty()) {
        return "key:$apiKey"
    }
    val forwarded = request.getHeader("X-Forwarded-For")?.split(",")?.firstOrNull()?.trim()
    val ip = forwarded?.takeIf { it.isNotEmpty() } ?: request.remoteAddr ?: "unknown"
    return "ip:$ip"
}

internal fun constantTimeEquals(
    left: String,
    right: String,
): Boolean {
    val a = left.toByteArray(StandardCharsets.UTF_8)
    val b = right.toByteArray(StandardCharsets.UTF_8)
    return MessageDigest.isEqual(a, b)
}
