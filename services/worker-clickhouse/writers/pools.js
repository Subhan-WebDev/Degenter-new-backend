// services/worker-clickhouse/writers/pools.js
import { chInsertJSON } from '../../../common/db-clickhouse.js';
import { info, warn } from '../../../common/log.js';
import { getPoolMeta, getTokenId } from '../lib/pool-resolver.js';

const poolBuffer = [];
const tokenBuffer = [];
const MAX_BUFFER = Number(process.env.CLICKHOUSE_POOL_BUFFER || 200);
const FLUSH_MS   = Number(process.env.CLICKHOUSE_POOL_FLUSH_MS || 2000);

function asDate(v) {
  const d = new Date(v);
  return isNaN(d.getTime()) ? new Date() : d;
}

async function pushPool(row) {
  poolBuffer.push(row);
  if (poolBuffer.length >= MAX_BUFFER) {
    await flushPools();
  }
}

async function pushToken(row) {
  tokenBuffer.push(row);
  if (tokenBuffer.length >= MAX_BUFFER) {
    await flushTokens();
  }
}

async function flushTokens() {
  if (!tokenBuffer.length) return;
  const batch = tokenBuffer.splice(0, tokenBuffer.length);
  await chInsertJSON({ table: 'tokens', rows: batch });
}

export async function flushPools() {
  if (poolBuffer.length) {
    const batch = poolBuffer.splice(0, poolBuffer.length);
    await chInsertJSON({ table: 'pools', rows: batch });
  }
  await flushTokens();
}

const seenTokens = new Set();

export async function handlePoolEvent(e) {
  try {
    const meta = await getPoolMeta(e.pair_contract, { retries: 20, delayMs: 500 });
    const pool_id = meta.pool_id || await getPoolMeta(e.pair_contract).then(m => m.pool_id);

    const base_id = meta.base_id || await getTokenId(meta.base_denom || e.base_denom);
    const quote_id = meta.quote_id || await getTokenId(meta.quote_denom || e.quote_denom);

    // Insert tokens once per id
    if (!seenTokens.has(base_id)) {
      await pushToken({
        token_id: base_id,
        denom: meta.base_denom || e.base_denom,
        type: '',
        name: '',
        symbol: '',
        display: '',
        exponent: Number(meta.base_exp ?? 0),
        image_uri: '',
        website: '',
        twitter: '',
        telegram: '',
        max_supply_base: '0',
        total_supply_base: '0',
        description: '',
        created_at: asDate(meta.created_at || e.created_at)
      });
      seenTokens.add(base_id);
    }

    if (!seenTokens.has(quote_id)) {
      await pushToken({
        token_id: quote_id,
        denom: meta.quote_denom || e.quote_denom,
        type: '',
        name: '',
        symbol: '',
        display: '',
        exponent: Number(meta.quote_exp ?? 0),
        image_uri: '',
        website: '',
        twitter: '',
        telegram: '',
        max_supply_base: '0',
        total_supply_base: '0',
        description: '',
        created_at: asDate(meta.created_at || e.created_at)
      });
      seenTokens.add(quote_id);
    }

    await pushPool({
      pool_id,
      pair_contract: e.pair_contract,
      base_token_id: base_id,
      quote_token_id: quote_id,
      lp_token_denom: '',
      pair_type: meta.pair_type || e.pair_type || 'xyk',
      is_uzig_quote: meta.is_uzig_quote ? 1 : 0,
      factory_contract: process.env.FACTORY_ADDR || '',
      router_contract: process.env.ROUTER_ADDR || '',
      created_at: asDate(e.created_at || meta.created_at),
      created_height: Number(e.height || 0),
      created_tx_hash: e.tx_hash || '',
      signer: e.signer || meta.signer || ''
    });

    info('[ch] pool recorded', e.pair_contract, pool_id);
  } catch (err) {
    warn('[ch/pool]', err?.message || err);
  }
}

setInterval(() => { flushPools().catch(()=>{}); }, FLUSH_MS);
