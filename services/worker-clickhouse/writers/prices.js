// services/worker-clickhouse/writers/prices.js
import { chInsertJSON } from '../../../common/db-clickhouse.js';
import { priceFromReserves_UZIGQuote } from '../../../common/core/prices.js';
import { warn } from '../../../common/log.js';

const buffer = [];
const MAX_BUFFER = Number(process.env.CLICKHOUSE_PRICE_BUFFER || 500);
const FLUSH_MS   = Number(process.env.CLICKHOUSE_PRICE_FLUSH_MS || 2000);

function asDate(v) {
  const d = new Date(v);
  return isNaN(d.getTime()) ? new Date() : d;
}

export async function flushPriceTicks() {
  if (!buffer.length) return;
  const batch = buffer.splice(0, buffer.length);
  await chInsertJSON({ table: 'price_ticks', rows: batch });
}

async function pushTick(row) {
  buffer.push(row);
  if (buffer.length >= MAX_BUFFER) {
    await flushPriceTicks();
  }
}

export async function pushPriceFromReserves(meta, reserves, at) {
  try {
    if (!meta?.is_uzig_quote) return;
    const price = priceFromReserves_UZIGQuote(
      { base_denom: meta.base_denom, base_exp: Number(meta.base_exp ?? 0) },
      reserves || []
    );

    if (price != null && Number.isFinite(price) && price > 0) {
      await pushTick({
        pool_id: meta.pool_id,
        token_id: meta.base_id,
        price_in_zig: price,
        ts: asDate(at)
      });
    }
  } catch (err) {
    warn('[ch/price]', err?.message || err);
  }
}

setInterval(() => { flushPriceTicks().catch(()=>{}); }, FLUSH_MS);
