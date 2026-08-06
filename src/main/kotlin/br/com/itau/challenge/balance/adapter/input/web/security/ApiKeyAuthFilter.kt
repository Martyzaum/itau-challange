package br.com.itau.challenge.balance.adapter.input.web.security

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import tools.jackson.databind.ObjectMapper

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
class ApiKeyAuthFilter(
    private val properties: ApiSecurityProperties,
    private val objectMapper: ObjectMapper,
) : OncePerRequestFilter() {

    override fun shouldNotFilter(request: HttpServletRequest): Boolean {
        if (!properties.auth.enabled) {
            return true
        }
        return ApiSecurityPaths.isPublic(request.requestURI)
    }

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val provided = request.getHeader(properties.auth.header)?.trim().orEmpty()
        val accepted = properties.auth.parsedKeys()
        if (provided.isEmpty() || accepted.none { constantTimeEquals(it, provided) }) {
            writeApiError(
                response = response,
                objectMapper = objectMapper,
                status = HttpStatus.UNAUTHORIZED.value(),
                code = "UNAUTHORIZED",
                message = "Missing or invalid API key",
            )
            return
        }
        filterChain.doFilter(request, response)
    }
}
