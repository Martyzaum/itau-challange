package br.com.itau.challenge.load

import io.gatling.javaapi.core.ClosedInjectionStep
import io.gatling.javaapi.core.CoreDsl.constantConcurrentUsers
import io.gatling.javaapi.core.CoreDsl.constantUsersPerSec
import io.gatling.javaapi.core.CoreDsl.details
import io.gatling.javaapi.core.CoreDsl.exec
import io.gatling.javaapi.core.CoreDsl.feed
import io.gatling.javaapi.core.CoreDsl.global
import io.gatling.javaapi.core.CoreDsl.jsonFile
import io.gatling.javaapi.core.CoreDsl.pace
import io.gatling.javaapi.core.CoreDsl.rampConcurrentUsers
import io.gatling.javaapi.core.CoreDsl.rampUsersPerSec
import io.gatling.javaapi.core.CoreDsl.scenario
import io.gatling.javaapi.core.OpenInjectionStep
import io.gatling.javaapi.core.ScenarioBuilder
import io.gatling.javaapi.core.Simulation
import io.gatling.javaapi.http.HttpDsl.http
import io.gatling.javaapi.http.HttpDsl.status
import java.time.Duration

/**
 * Load simulation for GET /balances/{accountId} — knobs inspired by k6.
 *
 * | prop | Make var | default | meaning |
 * |------|----------|---------|---------|
 * | baseUrl | BASE_URL | http://localhost:8080 | target |
 * | profile | PROFILE | custom | smoke \| load \| stress \| spike \| custom |
 * | vus | VUS / WORKERS | 20 | concurrent workers (closed model) |
 * | rps | RPS | — | if set → open model (arrival rate) |
 * | duration | DURATION | 30s | steady hold (`30`, `30s`, `1m`, `2m30s`) |
 * | rampUp | RAMP / RAMP_UP | 10s | ramp to target |
 * | rampDown | RAMP_DOWN | 0s | ramp down (closed only) |
 * | thinkMs | THINK_MS | 0 | pause between requests per VU (closed) |
 * | p99Ms | P99_MS | 2000 | assertion p99 |
 * | maxFailPct | MAX_FAIL_PCT | 1.0 | max failed % |
 * | cacheMode | CACHE_MODE | off | report label |
 *
 * Feeder: `account-ids.json` → `{ "accountId": "<uuid>" }[]`
 */
class GetBalanceSimulation : Simulation() {
    private val profile = prop("profile", "custom").lowercase()
    private val baseUrl = prop("baseUrl", "http://localhost:8080")
    private val cacheMode = prop("cacheMode", "off")
    private val thinkMs = (intProp("thinkMs", null) ?: 0).toLong()
    private val p99Ms = intProp("p99Ms", null) ?: 2000
    private val maxFailPct = doubleProp("maxFailPct", null) ?: 1.0
    private val resolved = resolveProfile(profile)
    private val openModel = resolved.rps != null

    private val httpProtocol =
        http
            .baseUrl(baseUrl)
            .acceptHeader("application/json")
            .userAgentHeader("gatling-itau-balance/cache-$cacheMode/profile-$profile")
            .shareConnections()

    private val accountFeeder = jsonFile("account-ids.json").random()

    private val getBalance =
        exec(
            http("GET /balances/{accountId}")
                .get("/balances/#{accountId}")
                .check(status().shouldBe(200)),
        )

    private val requestChain =
        if (thinkMs > 0) {
            feed(accountFeeder).exec(getBalance).pace(Duration.ofMillis(thinkMs))
        } else {
            feed(accountFeeder).exec(getBalance)
        }

    private val totalHold: Duration =
        resolved.rampUp.plus(resolved.duration).plus(resolved.rampDown)

    private val scn: ScenarioBuilder =
        if (openModel) {
            // Open model: each injected virtual user fires one request.
            scenario("get-balance-$profile-cache-$cacheMode")
                .exec(requestChain)
        } else {
            // Closed model: N workers loop for the whole window (k6 VUs).
            scenario("get-balance-$profile-cache-$cacheMode")
                .during(totalHold)
                .on(requestChain)
        }

    init {
        require(resolved.vus > 0) { "vus/workers must be > 0" }
        require(!resolved.duration.isZero && !resolved.duration.isNegative) { "duration must be > 0" }
        require(!resolved.rampUp.isNegative) { "rampUp must be >= 0" }
        require(!resolved.rampDown.isNegative) { "rampDown must be >= 0" }
        require(p99Ms > 0) { "p99Ms must be > 0" }
        require(maxFailPct in 0.0..100.0) { "maxFailPct must be 0..100" }
        resolved.rps?.let { require(it > 0.0) { "rps must be > 0 when set" } }

        val population =
            if (openModel) {
                scn.injectOpen(*openSteps(resolved))
            } else {
                scn.injectClosed(*closedSteps(resolved))
            }

        setUp(population)
            .protocols(httpProtocol)
            .maxDuration(totalHold.plusSeconds(10))
            .assertions(
                global().failedRequests().percent().lt(maxFailPct),
                global().responseTime().percentile3().lt(p99Ms),
                details("GET /balances/{accountId}").successfulRequests().percent().gt(100.0 - maxFailPct),
            )
    }

    private fun closedSteps(o: LoadOptions): Array<ClosedInjectionStep> {
        val steps = mutableListOf<ClosedInjectionStep>()
        if (!o.rampUp.isZero) {
            steps += rampConcurrentUsers(0).to(o.vus).during(o.rampUp)
        } else {
            steps += constantConcurrentUsers(o.vus).during(Duration.ofMillis(1))
        }
        steps += constantConcurrentUsers(o.vus).during(o.duration)
        if (!o.rampDown.isZero) {
            steps += rampConcurrentUsers(o.vus).to(0).during(o.rampDown)
        }
        return steps.toTypedArray()
    }

    private fun openSteps(o: LoadOptions): Array<OpenInjectionStep> {
        val rps = o.rps!!
        val steps = mutableListOf<OpenInjectionStep>()
        if (!o.rampUp.isZero) {
            steps += rampUsersPerSec(0.0).to(rps).during(o.rampUp)
        }
        steps += constantUsersPerSec(rps).during(o.duration)
        return steps.toTypedArray()
    }

    private data class LoadOptions(
        val vus: Int,
        val rps: Double?,
        val duration: Duration,
        val rampUp: Duration,
        val rampDown: Duration,
    )

    private fun resolveProfile(name: String): LoadOptions {
        val preset: LoadOptions =
            when (name) {
                "smoke" ->
                    LoadOptions(1, null, Duration.ofSeconds(15), Duration.ZERO, Duration.ZERO)
                "load" ->
                    LoadOptions(20, null, Duration.ofMinutes(1), Duration.ofSeconds(15), Duration.ofSeconds(10))
                "stress" ->
                    LoadOptions(100, null, Duration.ofMinutes(2), Duration.ofSeconds(30), Duration.ofSeconds(20))
                "spike" ->
                    LoadOptions(200, null, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofSeconds(5))
                "custom", "" ->
                    LoadOptions(20, null, Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ZERO)
                else ->
                    error("Unknown profile='$name' (use smoke|load|stress|spike|custom)")
            }

        return LoadOptions(
            vus = intProp("vus", null) ?: intProp("workers", null) ?: preset.vus,
            rps = doubleProp("rps", null) ?: preset.rps,
            duration = durationProp("duration", null) ?: preset.duration,
            rampUp = durationProp("rampUp", null) ?: durationProp("ramp", null) ?: preset.rampUp,
            rampDown = durationProp("rampDown", null) ?: preset.rampDown,
        )
    }

    companion object {
        private fun prop(
            key: String,
            default: String,
        ): String = System.getProperty(key, default).trim()

        private fun intProp(
            key: String,
            default: Int?,
        ): Int? {
            val raw = System.getProperty(key)?.trim().orEmpty()
            if (raw.isEmpty()) return default
            return raw.toInt()
        }

        private fun doubleProp(
            key: String,
            default: Double?,
        ): Double? {
            val raw = System.getProperty(key)?.trim().orEmpty()
            if (raw.isEmpty()) return default
            return raw.toDouble()
        }

        /** k6-style: `30`, `30s`, `1m`, `2m30s`, `1h`. */
        fun parseDuration(raw: String): Duration {
            val s = raw.trim().lowercase()
            require(s.isNotEmpty()) { "empty duration" }
            if (s.all { it.isDigit() }) {
                return Duration.ofSeconds(s.toLong())
            }
            val regex = Regex("""(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?""")
            val m =
                regex.matchEntire(s)
                    ?: error("Invalid duration '$raw' (use 30, 30s, 1m, 2m30s, 1h)")
            val hours = m.groupValues[1].toLongOrNull() ?: 0L
            val minutes = m.groupValues[2].toLongOrNull() ?: 0L
            val seconds = m.groupValues[3].toLongOrNull() ?: 0L
            require(hours + minutes + seconds > 0) { "Invalid duration '$raw'" }
            return Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds)
        }

        private fun durationProp(
            key: String,
            default: Duration?,
        ): Duration? {
            val raw = System.getProperty(key)?.trim().orEmpty()
            if (raw.isEmpty()) return default
            return parseDuration(raw)
        }
    }
}
