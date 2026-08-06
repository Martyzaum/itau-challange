package br.com.itau.challenge

import com.lemonappdev.konsist.api.Konsist
import com.lemonappdev.konsist.api.architecture.KoArchitectureCreator.assertArchitecture
import com.lemonappdev.konsist.api.architecture.Layer
import com.lemonappdev.konsist.api.verify.assertFalse
import org.junit.jupiter.api.Test

class HexagonalArchitectureTest {

    private val scope = Konsist.scopeFromPackage("br.com.itau.challenge..")

    private val domain = Layer("Domain", "..domain..")
    private val port = Layer("Port", "..port..")
    private val application = Layer("Application", "..application..")
    private val adapter = Layer("Adapter", "..adapter..")

    private val forbiddenInfraPrefixes =
        listOf(
            "org.springframework",
            "software.amazon",
            "org.apache.kafka",
            "org.springframework.kafka",
            "io.lettuce",
            "io.github.resilience4j",
            "io.micrometer",
            "io.opentelemetry",
            "tools.jackson",
            "com.fasterxml.jackson",
            "redis.clients",
            "software.amazon.awssdk",
        )

    @Test
    fun `hexagonal layers respect dependency direction`() {
        scope.assertArchitecture {
            domain.dependsOnNothing()
            port.doesNotDependOn(application, adapter)
            application.doesNotDependOn(adapter)
        }
    }

    @Test
    fun `domain does not depend on infrastructure frameworks`() {
        assertNoForbiddenImports("br.com.itau.challenge..domain..")
    }

    @Test
    fun `application does not depend on infrastructure frameworks`() {
        assertNoForbiddenImports("br.com.itau.challenge..application..")
    }

    @Test
    fun `port does not depend on infrastructure frameworks`() {
        assertNoForbiddenImports("br.com.itau.challenge..port..")
    }

    @Test
    fun `driving adapters do not depend on concrete application services`() {
        Konsist
            .scopeFromPackage("br.com.itau.challenge..adapter.input..")
            .files
            .assertFalse {
                it.hasImport { import ->
                    import.name.startsWith("br.com.itau.challenge.balance.application.") &&
                        import.name.endsWith("Service")
                }
            }
    }

    private fun assertNoForbiddenImports(packageName: String) {
        Konsist
            .scopeFromPackage(packageName)
            .files
            .assertFalse { file ->
                file.hasImport { import ->
                    forbiddenInfraPrefixes.any { prefix -> import.name.startsWith(prefix) }
                }
            }
    }
}
