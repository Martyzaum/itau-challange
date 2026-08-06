package br.com.itau.challenge.balance.adapter.output.redis

import br.com.itau.challenge.balance.domain.model.AccountBalance
import br.com.itau.challenge.balance.port.output.AccountBalanceCache
import br.com.itau.challenge.balance.port.output.CachePutResult
import br.com.itau.challenge.config.CircuitBreakerNames
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
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
    private val circuitBreaker: CircuitBreaker,
) : AccountBalanceCache {
    private val logger = LoggerFactory.getLogger(javaClass)

    override fun get(accountId: UUID): AccountBalance? =
        try {
            circuitBreaker.executeSupplier {
                val raw = commands.get(key(accountId)) ?: return@executeSupplier null
                objectMapper.readValue(raw, CachedAccountBalancePayload::class.java).toDomain()
            }
        } catch (ex: CallNotPermittedException) {
            logger.warn(
                "event=balance_cache_circuit_open dependency={} accountId={} op=get",
                CircuitBreakerNames.REDIS,
                accountId,
            )
            null
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_get_failed accountId={} error={}", accountId, ex.toString())
            null
        }

    override fun putIfNewer(balance: AccountBalance): CachePutResult =
        try {
            circuitBreaker.executeSupplier {
                val payload = objectMapper.writeValueAsString(CachedAccountBalancePayload.from(balance))
                val ttlSeconds =
                    if (ttl != null && !ttl.isZero && !ttl.isNegative) {
                        ttl.seconds.coerceAtLeast(1)
                    } else {
                        0L
                    }
                val written =
                    commands.eval<Long>(
                        PUT_IF_NEWER_LUA_SCRIPT,
                        ScriptOutputType.INTEGER,
                        arrayOf(key(balance.id)),
                        payload,
                        balance.updatedAtMicros.toString(),
                        balance.lastTransactionId.toString(),
                        ttlSeconds.toString(),
                    ) ?: 0L
                if (written == 1L) CachePutResult.WRITTEN else CachePutResult.REJECTED_NOT_NEWER
            }
        } catch (ex: CallNotPermittedException) {
            logger.warn(
                "event=balance_cache_circuit_open dependency={} accountId={} op=put",
                CircuitBreakerNames.REDIS,
                balance.id,
            )
            CachePutResult.FAILED
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_put_failed accountId={} error={}", balance.id, ex.toString())
            CachePutResult.FAILED
        }

    override fun invalidate(accountId: UUID) {
        try {
            circuitBreaker.executeSupplier {
                commands.del(key(accountId))
                null
            }
        } catch (ex: CallNotPermittedException) {
            logger.warn(
                "event=balance_cache_circuit_open dependency={} accountId={} op=invalidate",
                CircuitBreakerNames.REDIS,
                accountId,
            )
        } catch (ex: Exception) {
            logger.warn("event=balance_cache_invalidate_failed accountId={} error={}", accountId, ex.toString())
        }
    }

    override fun invalidateIfNotNewer(balance: AccountBalance) {
        try {
            circuitBreaker.executeSupplier {
                commands.eval<Long>(
                    INVALIDATE_IF_NOT_NEWER_LUA_SCRIPT,
                    ScriptOutputType.INTEGER,
                    arrayOf(key(balance.id)),
                    balance.updatedAtMicros.toString(),
                    balance.lastTransactionId.toString(),
                )
                null
            }
        } catch (ex: CallNotPermittedException) {
            logger.warn(
                "event=balance_cache_circuit_open dependency={} accountId={} op=invalidate_if_not_newer",
                CircuitBreakerNames.REDIS,
                balance.id,
            )
        } catch (ex: Exception) {
            logger.warn(
                "event=balance_cache_invalidate_failed accountId={} error={}",
                balance.id,
                ex.toString(),
            )
        }
    }

    private fun key(accountId: UUID): String = "$keyPrefix$accountId"

    companion object {
        const val PUT_IF_NEWER_LUA_SCRIPT = """
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

        /**
         * DEL only when cached version <= candidate (failed put). Preserves a concurrent newer write.
         * Missing/corrupt key → DEL (heal).
         */
        const val INVALIDATE_IF_NOT_NEWER_LUA_SCRIPT = """
local key = KEYS[1]
local candTs = tonumber(ARGV[1])
local candTxId = ARGV[2]

local current = redis.call('GET', key)
if not current then
  return 0
end

local ok, obj = pcall(cjson.decode, current)
if not ok or type(obj) ~= 'table' then
  redis.call('DEL', key)
  return 1
end

local curTs = tonumber(obj['updatedAtMicros'])
local curTxId = obj['lastTransactionId']
if curTs == nil or type(curTxId) ~= 'string' then
  redis.call('DEL', key)
  return 1
end

-- delete if current is NOT newer than candidate
if candTs > curTs or (candTs == curTs and candTxId >= curTxId) then
  redis.call('DEL', key)
  return 1
end

return 0
"""
    }
}
