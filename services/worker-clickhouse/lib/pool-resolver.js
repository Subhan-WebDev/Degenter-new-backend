// services/worker-clickhouse/lib/pool-resolver.js
import { createRedisClient } from '../../../common/redis-client.js';

const { client: redis, connect } = createRedisClient('ch-pool-cache');
const redisReady = connect();

const poolMetaCache = new Map();
const tokenIdCache = new Map();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchPoolMeta(pair_contract) {
  await redisReady;
  const raw = await redis.get(`pool_meta:${pair_contract}`);
  if (!raw) return null;
  try {
    const meta = JSON.parse(raw);
    poolMetaCache.set(pair_contract, meta);
    return meta;
  } catch {
    return null;
  }
}

async function fetchPoolId(pair_contract) {
  await redisReady;
  const raw = await redis.get(`pool_id:${pair_contract}`);
  if (!raw) return null;
  const id = Number(raw);
  if (Number.isFinite(id)) {
    const cached = poolMetaCache.get(pair_contract) || { pair_contract };
    cached.pool_id = id;
    poolMetaCache.set(pair_contract, cached);
    return id;
  }
  return null;
}

async function fetchTokenId(denom) {
  await redisReady;
  const raw = await redis.get(`token_id:${denom}`);
  if (!raw) return null;
  const id = Number(raw);
  if (Number.isFinite(id)) {
    tokenIdCache.set(denom, id);
    return id;
  }
  return null;
}

async function waitUntil(fn, { retries = 10, delayMs = 500 } = {}) {
  let last = null;
  for (let i = 0; i <= retries; i++) {
    // eslint-disable-next-line no-await-in-loop
    last = await fn();
    if (last) return last;
    if (i === retries) break;
    // eslint-disable-next-line no-await-in-loop
    await sleep(delayMs);
  }
  return last;
}

export async function getPoolMeta(pair_contract, opts = {}) {
  const cached = poolMetaCache.get(pair_contract);
  if (cached) return cached;

  const meta = await waitUntil(() => fetchPoolMeta(pair_contract), opts);
  if (meta) return meta;

  throw new Error(`pool meta not ready for ${pair_contract}`);
}

export async function getPoolId(pair_contract, opts = {}) {
  const cached = poolMetaCache.get(pair_contract);
  if (cached?.pool_id) return cached.pool_id;

  const id = await waitUntil(() => fetchPoolId(pair_contract), opts);
  if (id != null) return id;

  throw new Error(`pool id not ready for ${pair_contract}`);
}

export async function getTokenId(denom, opts = {}) {
  if (tokenIdCache.has(denom)) return tokenIdCache.get(denom);

  const id = await waitUntil(() => fetchTokenId(denom), opts);
  if (id != null) return id;

  throw new Error(`token id not ready for ${denom}`);
}
