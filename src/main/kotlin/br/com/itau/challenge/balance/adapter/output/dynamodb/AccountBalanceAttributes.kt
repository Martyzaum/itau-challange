package br.com.itau.challenge.balance.adapter.output.dynamodb

object AccountBalanceAttributes {
    const val ACCOUNT_ID = "account_id"
    const val OWNER = "owner"
    const val BALANCE_AMOUNT = "balance_amount"
    const val BALANCE_CURRENCY = "balance_currency"
    const val UPDATED_AT_MICROS = "updated_at_micros"
    const val LAST_TRANSACTION_ID = "last_transaction_id"

    const val CONDITION_SAVE_IF_NEWER =
        "attribute_not_exists(#accountId) " +
            "OR #updatedAt < :newUpdatedAt " +
            "OR (#updatedAt = :newUpdatedAt AND (attribute_not_exists(#lastTxId) OR #lastTxId < :newLastTxId))"
}
