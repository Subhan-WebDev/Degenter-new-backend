// services/worker-metadata/tasks/fasttrack-consumer.js
import { createClient } from 'redis';
import { info, warn, debug } from '../../../common/log.js';
import { TS as DB } from '../../../common/db-timescale.js';

import { refreshMetaOnce } from './meta-refresher.js';
import { refreshHoldersOnce } from './holders-refresher.js';
import { scanTokenOnce } from './token-security.js';
import { refreshPoolMatrixOnce, refreshTokenMatrixOnce } from './matrix-rollups.js';

import { fetchPoolReserves, priceFromReserves_UZIGQuote, upsertPrice } from '../../../common/core/prices.js';
import { upsertOHLCV1m } from '../../../common/core/ohlcv.js';

const STREAM = process.env.FT_STREAM || 'events:new_pool';
const GROUP  = process.env.FT_GROUP  || 'metadata';
const CONSUMER = `${process.env.SVC_NAME || 'local'}-fasttrack-${Math.random().toString(36).slice(2,8)}`;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function minuteFloor(d) {
  const t = new Date(d instanceof Date ? d : new Date(d));
  t.setSeconds(0, 0);
  return t;
}

async function ensureGroup(client) {
  try {
    await client.xGroupCreate(STREAM, GROUP, '0', { MKSTREAM: true });
  } catch (e) {
    if (!String(e?.message || '').includes('BUSYGROUP')) throw e;
  }
}

async function loadPoolContextById(pool_id) {
  const { rows } = await DB.query(`
    SELECT
      p.pool_id, p.pair_contract, p.is_uzig_quote, p.created_at,
      p.base_token_id, b.denom AS base_denom,
      p.quote_token_id, q.denom AS quote_denom
    FROM pools p
    JOIN tokens b ON b.token_id=p.base_token_id
    JOIN tokens q ON q.token_id=p.quote_token_id
    WHERE p.pool_id = $1
  `, [pool_id]);
  return rows[0] || null;
}

export function startFasttrackConsumer() {
  (async () => {
    const url = process.env.REDIS_URL || 'redis://localhost:6379';
    const r = createClient({ url });
    r.on('error', (e) => warn('[fasttrack/redis]', e.message));
    await r.connect();
    await ensureGroup(r);

    info('[fasttrack] listening', { stream: STREAM, group: GROUP, consumer: CONSUMER });

    while (true) {
      try {
        const resp = await r.xReadGroup(GROUP, CONSUMER, [{ key: STREAM, id: '>' }], {
          COUNT: 16,
          BLOCK: 5000
        });
        if (!resp) continue;

        for (const { messages } of resp) {
          for (const m of messages) {
            try {
              const data = Object.fromEntries(
                // stream values are [k1,v1,k2,v2,...]
                (m.message || m.value || []).map ? [] : []
              );

              // robustly parse map (redis client returns object already)
              const payload = m.message || m.value || {};
              const pool_id = Number(payload.pool_id);
              const ctx = await loadPoolContextById(pool_id);
              if (!ctx) { warn('[fasttrack] no context for pool_id', pool_id); await r.xAck(STREAM, GROUP, m.id); continue; }

              info('[fasttrack] new_pool', { pool_id: ctx.pool_id, pair: ctx.pair_contract, base: ctx.base_denom, quote: ctx.quote_denom });

              // 1) metadata
              await Promise.allSettled([
                refreshMetaOnce(ctx.base_denom),
                refreshMetaOnce(ctx.quote_denom),
              ]);

              // 2) holders (base + non-uzig quote)
              await Promise.allSettled([
                refreshHoldersOnce(ctx.base_token_id, ctx.base_denom),
                (ctx.quote_denom !== 'uzig' ? refreshHoldersOnce(ctx.quote_token_id, ctx.quote_denom) : Promise.resolve())
              ]);

              // 3) security
              await Promise.allSettled([
                scanTokenOnce(ctx.base_token_id, ctx.base_denom),
                (ctx.quote_denom !== 'uzig' ? scanTokenOnce(ctx.quote_token_id, ctx.quote_denom) : Promise.resolve())
              ]);

              // 4) matrix (pool + tokens)
              await Promise.allSettled([
                refreshPoolMatrixOnce(ctx.pool_id),
                refreshTokenMatrixOnce(ctx.base_token_id),
                (ctx.quote_denom !== 'uzig' ? refreshTokenMatrixOnce(ctx.quote_token_id) : Promise.resolve()),
              ]);

              // 5) initial price + OHLCV seed (UZIG-quote only)
              try {
                if (ctx.quote_denom === 'uzig') {
                  const { rows: rExp } = await DB.query('SELECT exponent AS exp FROM tokens WHERE token_id=$1', [ctx.base_token_id]);
                  const baseExp = rExp?.[0]?.exp;
                  if (baseExp == null) {
                    debug('[fasttrack/init skip] meta not ready', { pool_id: ctx.pool_id, denom: ctx.base_denom });
                  } else {
                    const reserves = await fetchPoolReserves(ctx.pair_contract);
                    const price = priceFromReserves_UZIGQuote(
                      { base_denom: ctx.base_denom, base_exp: Number(baseExp) },
                      reserves
                    );
                    if (price && Number.isFinite(price) && price > 0) {
                      await upsertPrice(ctx.base_token_id, ctx.pool_id, price, true);
                      const bucket = minuteFloor(ctx.created_at);
                      await upsertOHLCV1m({
                        pool_id: ctx.pool_id,
                        bucket_start: bucket,
                        price,
                        vol_zig: 0,
                        trade_inc: 0
                      });
                      debug('[fasttrack/init seed]', { pool_id: ctx.pool_id, price, bucket: bucket.toISOString() });
                    }
                  }
                }
              } catch (e) { warn('[fasttrack/init]', e.message); }

              await r.xAck(STREAM, GROUP, m.id);
            } catch (e) {
              warn('[fasttrack/msg]', e.message);
              // don't ack to retry later
            }
          }
        }
      } catch (e) {
        warn('[fasttrack/loop]', e.message);
        await sleep(1000);
      }
    }
  })().catch(()=>{});
}
