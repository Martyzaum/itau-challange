package br.com.itau.challenge.balance.adapter.input.web

import br.com.itau.challenge.balance.adapter.input.web.dto.ApiErrorResponse
import br.com.itau.challenge.balance.domain.exception.AccountBalanceNotFoundException
import br.com.itau.challenge.balance.domain.exception.DependencyUnavailableException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import java.util.UUID

@RestControllerAdvice
class BalanceExceptionHandler {

    private val logger = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(AccountBalanceNotFoundException::class)
    fun handleNotFound(exception: AccountBalanceNotFoundException): ResponseEntity<ApiErrorResponse> =
        ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(
                ApiErrorResponse(
                    code = "ACCOUNT_BALANCE_NOT_FOUND",
                    message = exception.message ?: "Account balance not found",
                ),
            )

    @ExceptionHandler(DependencyUnavailableException::class)
    fun handleDependencyUnavailable(exception: DependencyUnavailableException): ResponseEntity<ApiErrorResponse> =
        ResponseEntity
            .status(HttpStatus.SERVICE_UNAVAILABLE)
            .header(HttpHeaders.RETRY_AFTER, "30")
            .body(
                ApiErrorResponse(
                    code = "DEPENDENCY_UNAVAILABLE",
                    message = exception.message ?: "Dependency unavailable",
                ),
            )

    @ExceptionHandler(MethodArgumentTypeMismatchException::class)
    fun handleTypeMismatch(exception: MethodArgumentTypeMismatchException): ResponseEntity<ApiErrorResponse>? {
        if (exception.requiredType != UUID::class.java) {
            return null
        }

        return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(
                ApiErrorResponse(
                    code = "INVALID_ACCOUNT_ID",
                    message = "accountId must be a valid UUID",
                ),
            )
    }

    @ExceptionHandler(Exception::class)
    fun handleUnexpected(exception: Exception): ResponseEntity<ApiErrorResponse> {
        logger.error("event=unhandled_exception type={}", exception.javaClass.name, exception)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(
                ApiErrorResponse(
                    code = "INTERNAL_ERROR",
                    message = "Unexpected server error",
                ),
            )
    }
}
