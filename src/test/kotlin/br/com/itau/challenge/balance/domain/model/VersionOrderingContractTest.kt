package br.com.itau.challenge.balance.domain.model

import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Single source of truth for newest-wins ordering used by domain, Dynamo condition and Redis Lua.
 * Keep this matrix green when changing any of the three gates.
 */
class VersionOrderingContractTest {

    private val money = Balance(BigDecimal("10.00"), "BRL")
    private val lowerTx = UUID.fromString("00000000-0000-4000-8000-000000000001")
    private val higherTx = UUID.fromString("ffffffff-ffff-4fff-8fff-ffffffffffff")
    private val accountId = UUID.fromString("5b19c8b6-0cc4-4c72-a989-0c2ee15fa975")
    private val ownerId = UUID.fromString("315e3cfe-f4af-4cd2-b298-a449e614349a")

    @Test
    fun `domain isNewerThan matches composite version matrix`() {
        val base = snap(100, lowerTx)
        assertTrue(snap(200, lowerTx).isNewerThan(base))
        assertFalse(snap(50, higherTx).isNewerThan(base))
        assertTrue(snap(100, higherTx).isNewerThan(base))
        assertFalse(snap(100, lowerTx).isNewerThan(base))
        assertFalse(base.isNewerThan(base))
    }

    @Test
    fun `matrix rows are stable for documentation`() {
        val rows =
            listOf(
                Triple(200L, lowerTx, true),
                Triple(50L, higherTx, false),
                Triple(100L, higherTx, true),
                Triple(100L, lowerTx, false),
            )
        val base = snap(100, lowerTx)
        rows.forEach { (ts, tx, expectedNewer) ->
            assertEquals(expectedNewer, snap(ts, tx).isNewerThan(base), "ts=$ts tx=$tx")
        }
    }

    private fun snap(
        ts: Long,
        tx: UUID,
    ) = AccountBalance(
        id = accountId,
        owner = ownerId,
        balance = money,
        updatedAtMicros = ts,
        lastTransactionId = tx,
    )
}
