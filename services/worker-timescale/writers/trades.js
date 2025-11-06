// services/worker-timescale/writers/trades.js
import { poolWithTokens } from '../../../common/core/pools.js';
import { insertTrade } from '../../../common/core/trades.js';
import { upsertPoolState } from '../../../common/core/pool_state.js';
import { upsertOHLCV1m } from '../../../common/core/ohlcv.js';
import { upsertPrice, priceFromReserves_UZIGQuote } from '../../../common/core/prices.js';
import { classifyDirection, digitsOrNull } from '../../../common/core/parse.js';
import { info, warn, debug } from '../../../common/log.js';

/**
 * swap event:
 * {
 *   pair_contract, action:'swap',
 *   offer_asset_denom, offer_amount_base,
 *   ask_asset_denom,   ask_amount_base,
 *   return_amount_base,
 *   is_router,
 *   reserve_asset1_denom, reserve_asset1_amount_base,
 *   reserve_asset2_denom, reserve_asset2_amount_base,
 *   height, tx_hash, signer, msg_index, created_at
 * }
 */
export async function handleSwapEvent(e) {
  try {
    const pool = await poolWithTokens(e.pair_contract);
    if (!pool) { warn('[ts/swap] unknown pool', e.pair_contract); return; }

    // direction depends on pool.quote_denom
    const direction = classifyDirection(e.offer_asset_denom, pool.quote_denom);

    await insertTrade({
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      action: 'swap',
      direction,
      offer_asset_denom: e.offer_asset_denom,
      offer_amount_base: e.offer_amount_base,
      ask_asset_denom:   e.ask_asset_denom,
      ask_amount_base:   e.ask_amount_base,
      return_amount_base: e.return_amount_base,
      is_router: !!e.is_router,
      reserve_asset1_denom: e.reserve_asset1_denom,
      reserve_asset1_amount_base: e.reserve_asset1_amount_base,
      reserve_asset2_denom: e.reserve_asset2_denom,
      reserve_asset2_amount_base: e.reserve_asset2_amount_base,
      height: e.height,
      tx_hash: e.tx_hash,
      signer: e.signer,
      msg_index: e.msg_index,
      created_at: e.created_at
    });

    // Update live pool_state when reserves present
    await upsertPoolState(
      pool.pool_id,
      pool.base_denom,
      pool.quote_denom,
      e.reserve_asset1_denom,
      e.reserve_asset1_amount_base,
      e.reserve_asset2_denom,
      e.reserve_asset2_amount_base
    );

    // Price + OHLCV when UZIG quote and reserves are present
    if (pool.is_uzig_quote &&
        e.reserve_asset1_denom && e.reserve_asset1_amount_base &&
        e.reserve_asset2_denom && e.reserve_asset2_amount_base) {

      const reserves = [
        { denom: e.reserve_asset1_denom, amount_base: e.reserve_asset1_amount_base },
        { denom: e.reserve_asset2_denom, amount_base: e.reserve_asset2_amount_base }
      ];
      const price = priceFromReserves_UZIGQuote(
        { base_denom: pool.base_denom, base_exp: Number(pool.base_exp) },
        reserves
      );

      if (price != null && Number.isFinite(price) && price > 0) {
        // Compute minute bucket
        const bucket = new Date(Math.floor(new Date(e.created_at).getTime() / 60000) * 60000);

        // Volume in ZIG (check if offer was quote or return was quote)
        const quoteRaw = (e.offer_asset_denom === pool.quote_denom)
          ? Number(e.offer_amount_base || 0)
          : Number(e.return_amount_base || 0);
        const volZig = quoteRaw / 1e6; // UZIG exponent always 6

        await upsertOHLCV1m({
          pool_id: pool.pool_id,
          bucket_start: bucket,
          price,
          vol_zig: volZig,
          trade_inc: 1
        });

        await upsertPrice(pool.base_id, pool.pool_id, price, true);
      }
    }
  } catch (err) {
    warn('[ts/swap]', err?.message || err);
  }
}

/**
 * liquidity event:
 * {
 *   pair_contract, action: 'provide'|'withdraw',
 *   share_base,
 *   reserve_asset1_denom, reserve_asset1_amount_base,
 *   reserve_asset2_denom, reserve_asset2_amount_base,
 *   height, tx_hash, signer, msg_index, created_at
 * }
 */
export async function handleLiquidityEvent(e) {
  try {
    const pool = await poolWithTokens(e.pair_contract);
    if (!pool) return;

    await insertTrade({
      pool_id: pool.pool_id,
      pair_contract: e.pair_contract,
      action: e.action,
      direction: e.action, // keep your schema semantics (provide/withdraw)
      offer_asset_denom: null,
      offer_amount_base: null,
      ask_asset_denom: null,
      ask_amount_base: null,
      return_amount_base: e.share_base || null,
      is_router: false,
      reserve_asset1_denom: e.reserve_asset1_denom,
      reserve_asset1_amount_base: e.reserve_asset1_amount_base,
      reserve_asset2_denom: e.reserve_asset2_denom,
      reserve_asset2_amount_base: e.reserve_asset2_amount_base,
      height: e.height,
      tx_hash: e.tx_hash,
      signer: e.signer,
      msg_index: e.msg_index,
      created_at: e.created_at
    });

    // Update live pool_state when reserves present
    await upsertPoolState(
      pool.pool_id,
      pool.base_denom,
      pool.quote_denom,
      e.reserve_asset1_denom,
      e.reserve_asset1_amount_base,
      e.reserve_asset2_denom,
      e.reserve_asset2_amount_base
    );

    // No OHLCV for liq (same as your original)
  } catch (err) {
    warn('[ts/liq]', err?.message || err);
  }
}
