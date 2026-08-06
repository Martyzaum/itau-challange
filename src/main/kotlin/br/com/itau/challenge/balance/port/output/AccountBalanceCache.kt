package br.com.itau.challenge.balance.port.output

import br.com.itau.challenge.balance.domain.model.AccountBalance
import java.util.UUID

enum class CachePutResult {
    WRITTEN,
    REJECTED_NOT_NEWER,
    FAILED,
}

interface AccountBalanceCache {
    fun get(accountId: UUID): AccountBalance?

    fun putIfNewer(balance: AccountBalance): CachePutResult

    /**
     * Best-effort delete. Prefer [invalidateIfNotNewer] after a failed put so a concurrent
     * newer snapshot is not removed.
     */
    fun invalidate(accountId: UUID)

    /**
     * Deletes the key only when the cached version is older than or equal to [balance]'s version.
     * Safe after a failed put of [balance]: will not wipe a concurrently written newer snapshot.
     */
    fun invalidateIfNotNewer(balance: AccountBalance)
}
