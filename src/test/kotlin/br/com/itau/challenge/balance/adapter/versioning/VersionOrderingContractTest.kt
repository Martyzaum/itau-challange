package br.com.itau.challenge.balance.adapter.versioning

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.domain.model.Balance
import br.com.itau.challenge.balance.domain.model.SnapshotVersion
import br.com.itau.challenge.balance.adapter.output.dynamodb.AccountBalanceAttributes
import br.com.itau.challenge.balance.adapter.output.redis.RedisAccountBalanceCache
import java.math.BigDecimal
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Single source of truth for newest-wins ordering used by domain, Dynamo condition and Redis Lua.
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
    fun `SnapshotVersion predicate matches domain isNewerThan`() {
        val cases =
            listOf(
                Case(200, lowerTx, 100, lowerTx, true),
                Case(50, higherTx, 100, lowerTx, false),
                Case(100, higherTx, 100, lowerTx, true),
                Case(100, lowerTx, 100, lowerTx, false),
            )
        cases.forEach { c ->
            val expected =
                SnapshotVersion.isNewerThan(
                    candidateTs = c.candidateTs,
                    candidateTxId = c.candidateTx.toString(),
                    currentTs = c.currentTs,
                    currentTxId = c.currentTx.toString(),
                )
            assertEquals(c.newer, expected, c.toString())
            assertEquals(
                expected,
                snap(c.candidateTs, c.candidateTx).isNewerThan(snap(c.currentTs, c.currentTx)),
                c.toString(),
            )
        }
    }

    @Test
    fun `dynamo condition expression encodes same composite gate`() {
        val expr = AccountBalanceAttributes.CONDITION_SAVE_IF_NEWER
        assertTrue(expr.contains("attribute_not_exists(#accountId)"))
        assertTrue(expr.contains("#updatedAt < :newUpdatedAt"))
        assertTrue(expr.contains("#updatedAt = :newUpdatedAt"))
        assertTrue(expr.contains("#lastTxId < :newLastTxId"))
    }

    @Test
    fun `redis lua encodes same composite gate`() {
        val lua = RedisAccountBalanceCache.PUT_IF_NEWER_LUA_SCRIPT
        assertTrue(lua.contains("newTs > curTs"))
        assertTrue(lua.contains("newTs == curTs and newTxId > curTxId"))
    }

    private data class Case(
        val candidateTs: Long,
        val candidateTx: UUID,
        val currentTs: Long,
        val currentTx: UUID,
        val newer: Boolean,
    )

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
