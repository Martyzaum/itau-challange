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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 30)
class RateLimitFilter(
    private val properties: ApiSecurityProperties,
    private val objectMapper: ObjectMapper,
) : OncePerRequestFilter() {

    private val windows = ConcurrentHashMap<String, Window>()

    override fun shouldNotFilter(request: HttpServletRequest): Boolean {
        if (!properties.rateLimit.enabled) {
            return true
        }
        return ApiSecurityPaths.isPublic(request.requestURI)
    }

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val limit = properties.rateLimit.requestsPerMinute.coerceAtLeast(1)
        val key = clientKey(request, properties.auth.header)
        val decision = allow(key, limit)
        response.setHeader("X-RateLimit-Limit", limit.toString())
        response.setHeader("X-RateLimit-Remaining", decision.remaining.toString())
        if (!decision.allowed) {
            response.setHeader("Retry-After", decision.retryAfterSeconds.toString())
            writeApiError(
                response = response,
                objectMapper = objectMapper,
                status = HttpStatus.TOO_MANY_REQUESTS.value(),
                code = "RATE_LIMIT_EXCEEDED",
                message = "Rate limit exceeded",
            )
            return
        }
        filterChain.doFilter(request, response)
    }

    private fun allow(
        key: String,
        limit: Int,
    ): Decision {
        val now = System.currentTimeMillis()
        val window =
            windows.compute(key) { _, current ->
                if (current == null || now - current.windowStartMs >= WINDOW_MS) {
                    Window(windowStartMs = now, count = AtomicInteger(0))
                } else {
                    current
                }
            }!!
        val count = window.count.incrementAndGet()
        val remaining = (limit - count).coerceAtLeast(0)
        if (count > limit) {
            val retryAfter = ((window.windowStartMs + WINDOW_MS - now + 999) / 1000).coerceAtLeast(1)
            return Decision(allowed = false, remaining = 0, retryAfterSeconds = retryAfter)
        }
        return Decision(allowed = true, remaining = remaining, retryAfterSeconds = 0)
    }

    private data class Window(
        val windowStartMs: Long,
        val count: AtomicInteger,
    )

    private data class Decision(
        val allowed: Boolean,
        val remaining: Int,
        val retryAfterSeconds: Long,
    )

    private companion object {
        const val WINDOW_MS = 60_000L
    }
}
