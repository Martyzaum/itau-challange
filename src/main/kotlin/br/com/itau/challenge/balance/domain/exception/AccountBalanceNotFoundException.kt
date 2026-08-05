package br.com.itau.challenge.balance.domain.exception

import java.util.UUID

class AccountBalanceNotFoundException(accountId: UUID) :
    RuntimeException("Account balance not found for account $accountId")
