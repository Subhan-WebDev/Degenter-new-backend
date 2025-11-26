// services/worker-timescale/writers/pools.js
import { createRedisClient } from '../../../common/redis-client.js';
import { upsertPool, poolWithTokens } from '../../../common/core/pools.js';
import { setTokenMetaFromLCD } from '../../../common/core/tokens.js';
import { info, warn, debug } from '../../../common/log.js';

const { client: redis, connect } = createRedisClient('ts-pool-cache');
const redisReady = connect();

async function cachePoolMapping(meta) {
  await redisReady;
  await redis.mSet({
    [`pool_id:${meta.pair_contract}`]: String(meta.pool_id),
    [`token_id:${meta.base_denom}`]: String(meta.base_id),
    [`token_id:${meta.quote_denom}`]: String(meta.quote_id),
    [`pool_meta:${meta.pair_contract}`]: JSON.stringify(meta)
  });
}

/**
 * Event shape (from processor):
 * {
 *   pair_contract, base_denom, quote_denom, pair_type,
 *   created_at, height, tx_hash, signer
 * }
 */
export async function handlePoolEvent(e) {
  try {
    const { pool_id, base_id, quote_id, is_uzig_quote } = await upsertPool({
      pairContract: e.pair_contract,
      baseDenom: e.base_denom,
      quoteDenom: e.quote_denom,
      pairType: e.pair_type,
      createdAt: e.created_at,
      height: e.height,
      txHash: e.tx_hash,
      signer: e.signer
    });

    const full = await poolWithTokens(e.pair_contract);
    const meta = full || {
      pool_id,
      pair_contract: e.pair_contract,
      base_id,
      quote_id,
      base_denom: e.base_denom,
      quote_denom: e.quote_denom,
      base_exp: 0,
      quote_exp: 0,
      pair_type: e.pair_type,
      is_uzig_quote
    };
    await cachePoolMapping(meta);

    // Fire-and-forget token metadata refresh (low priority)
    // (No await to keep writer fast)
    setTokenMetaFromLCD(e.base_denom).catch(()=>{});
    setTokenMetaFromLCD(e.quote_denom).catch(()=>{});

    info('[ts] pool upsert', e.pair_contract, 'pool_id=', pool_id);
  } catch (err) {
    warn('[ts/pool]', err?.message || err);
  }
}
