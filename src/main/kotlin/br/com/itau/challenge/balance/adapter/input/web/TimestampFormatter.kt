package br.com.itau.challenge.balance.adapter.input.web

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

private val SAO_PAULO = ZoneId.of("America/Sao_Paulo")
private val ISO_OFFSET = DateTimeFormatter.ISO_OFFSET_DATE_TIME

internal fun Long.toIso8601(zoneId: ZoneId = SAO_PAULO): String {
    val seconds = this / MICROS_PER_SECOND
    val nanos = (this % MICROS_PER_SECOND) * NANOS_PER_MICRO
    return Instant.ofEpochSecond(seconds, nanos).atZone(zoneId).format(ISO_OFFSET)
}

private const val MICROS_PER_SECOND = 1_000_000L
private const val NANOS_PER_MICRO = 1_000L
