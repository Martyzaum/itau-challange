package br.com.itau.challenge.balance.support

import org.awaitility.Awaitility
import org.awaitility.core.ConditionFactory
import java.time.Duration

object AwaitilitySupport {
    fun awaitAtMost(seconds: Long = 20): ConditionFactory =
        Awaitility
            .await()
            .atMost(Duration.ofSeconds(seconds))
            .pollInterval(Duration.ofMillis(250))
            .ignoreExceptions()
}
