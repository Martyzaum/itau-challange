package br.com.itau.challenge.balance.domain.exception

class DependencyUnavailableException(
    val dependency: String,
    cause: Throwable? = null,
) : RuntimeException("Dependency unavailable: $dependency", cause)
