package br.com.itau.challenge.balance

import br.com.itau.challenge.balance.adapter.input.kafka.TransactionEventConsumer
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

@SpringBootTest
class IngestionDisabledIntegrationTest(
    @Autowired private val applicationContext: ApplicationContext,
) {
    @Test
    fun `should not register transaction event consumer when ingestion is disabled`() {
        assertFalse(applicationContext.containsBean("transactionEventConsumer"))
        assertFailsWith<NoSuchBeanDefinitionException> {
            applicationContext.getBean(TransactionEventConsumer::class.java)
        }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun props(registry: DynamicPropertyRegistry) {
            registry.add("transactions.ingestion.enabled") { "false" }
            registry.add("spring.kafka.consumer.group-id") {
                "balance-ingestion-off-${UUID.randomUUID()}"
            }
            registry.add("balance.cache.enabled") { "false" }
            registry.add("api.auth.enabled") { "false" }
            registry.add("management.otlp.metrics.export.enabled") { "false" }
            registry.add("management.tracing.enabled") { "false" }
            registry.add("management.logging.export.otlp.enabled") { "false" }
        }
    }
}
