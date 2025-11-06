// services/worker-timescale/writers/pools.js
import { upsertPool, poolWithTokens } from '../../../common/core/pools.js';
import { setTokenMetaFromLCD } from '../../../common/core/tokens.js';
import { info, warn, debug } from '../../../common/log.js';

/**
 * Event shape (from processor):
 * {
 *   pair_contract, base_denom, quote_denom, pair_type,
 *   created_at, height, tx_hash, signer
 * }
 */
export async function handlePoolEvent(e) {
  try {
    const pool_id = await upsertPool({
      pairContract: e.pair_contract,
      baseDenom: e.base_denom,
      quoteDenom: e.quote_denom,
      pairType: e.pair_type,
      createdAt: e.created_at,
      height: e.height,
      txHash: e.tx_hash,
      signer: e.signer
    });

    // Fire-and-forget token metadata refresh (low priority)
    // (No await to keep writer fast)
    setTokenMetaFromLCD(e.base_denom).catch(()=>{});
    setTokenMetaFromLCD(e.quote_denom).catch(()=>{});

    info('[ts] pool upsert', e.pair_contract, 'pool_id=', pool_id);
  } catch (err) {
    warn('[ts/pool]', err?.message || err);
  }
}
