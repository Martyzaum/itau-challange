package br.com.itau.challenge.balance.adapter.input.web

import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TimestampFormatterTest {

    @Test
    fun `should convert micros to iso 8601 with sao paulo offset`() {
        val iso = 1_751_641_364_589_998L.toIso8601()

        assertTrue(iso.startsWith("2025-07-04T"))
        assertTrue(iso.contains("-03:00") || iso.contains("-02:00"))
    }

    @Test
    fun `should convert micros to utc when zone is utc`() {
        val iso = 1_751_641_364_589_998L.toIso8601(ZoneOffset.UTC)

        assertEquals("2025-07-04T15:02:44.589998Z", iso)
    }
}
