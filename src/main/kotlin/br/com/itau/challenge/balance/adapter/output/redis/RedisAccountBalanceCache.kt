package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import org.slf4j.LoggerFactory
import tools.jackson.databind.ObjectMapper
import java.time.Duration
import java.util.UUID

class RedisAccountBalanceCache(
    private val commands: RedisCommands<String, String>,
    private val objectMapper: ObjectMapper,
    private val keyPrefix: String,
    private val ttl: Duration?,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun get(accountId: UUID): AccountBalance? =
        try {
            val raw = commands.get(key(accountId)) ?: return null
            objectMapper.readValue(raw, CachedAccountBalancePayload::class.java).toDomain()
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_get_failed accountId={} error={}", accountId, ex.toString())
            null
        }

    fun putIfNewer(balance: AccountBalance) {
        try {
            val payload = objectMapper.writeValueAsString(CachedAccountBalancePayload.from(balance))
            val ttlSeconds =
                if (ttl != null && !ttl.isZero && !ttl.isNegative) {
                    ttl.seconds.coerceAtLeast(1)
                } else {
                    0L
                }
            commands.eval<Long>(
                PUT_IF_NEWER_LUA,
                ScriptOutputType.INTEGER,
                arrayOf(key(balance.id)),
                payload,
                balance.updatedAtMicros.toString(),
                balance.lastTransactionId.toString(),
                ttlSeconds.toString(),
            )
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_put_failed accountId={} error={}", balance.id, ex.toString())
        }
    }

    private fun key(accountId: UUID): String = "$keyPrefix$accountId"

    private companion object {
        const val PUT_IF_NEWER_LUA = """
local key = KEYS[1]
local newPayload = ARGV[1]
local newTs = tonumber(ARGV[2])
local newTxId = ARGV[3]
local ttl = tonumber(ARGV[4])

local function write()
  if ttl ~= nil and ttl > 0 then
    redis.call('SETEX', key, ttl, newPayload)
  else
    redis.call('SET', key, newPayload)
  end
end

local current = redis.call('GET', key)
if not current then
  write()
  return 1
end

local ok, obj = pcall(cjson.decode, current)
if not ok or type(obj) ~= 'table' then
  write()
  return 1
end

local curTs = tonumber(obj['updatedAtMicros'])
local curTxId = obj['lastTransactionId']
if curTs == nil or type(curTxId) ~= 'string' then
  write()
  return 1
end

if newTs > curTs or (newTs == curTs and newTxId > curTxId) then
  write()
  return 1
end

return 0
"""
    }
}
