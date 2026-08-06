package br.com.itau.challenge.balance.domain.model

/**
 * Canonical newest-wins ordering mirrored by Dynamo condition and Redis Lua.
 */
object SnapshotVersion {
    fun isNewerThan(
        candidateTs: Long,
        candidateTxId: String,
        currentTs: Long,
        currentTxId: String,
    ): Boolean {
        if (candidateTs != currentTs) {
            return candidateTs > currentTs
        }
        return candidateTxId > currentTxId
    }
}
