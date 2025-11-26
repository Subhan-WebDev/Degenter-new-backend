// services/processor-core/index.js
import '../../common/load-env.js';
process.env.SVC_NAME = process.env.SVC_NAME || 'processor-core';

import { createRedisClient } from '../../common/redis-client.js';
import { createConsumerGroup, readLoop } from '../../common/streams.js';
import { info, warn, err } from '../../common/log.js';

import { parseBlock } from './parser/parse.js';

const STREAM_IN   = process.env.STREAM_RAW || 'chain:raw_blocks';
const GROUP       = process.env.STREAM_GROUP || 'processor';
const CONSUMER    = process.env.CONSUMER || `proc-${Math.random().toString(36).slice(2,8)}`;

const STREAM_OUTS = {
  new_pool:  process.env.STREAM_NEW_POOL  || 'events:new_pool',
  swap:      process.env.STREAM_SWAP      || 'events:swap',
  liquidity: process.env.STREAM_LIQUIDITY || 'events:liquidity'
};

const { client: redis, connect: redisConnect } = createRedisClient('processor');

async function emit(stream, obj) {
  return redis.xAdd(stream, '*', { j: JSON.stringify(obj) });
}

async function handler(records, { ackMany }) {
  const ids = [];
  for (const rec of records) {
    ids.push(rec.id);
    try {
      const raw = rec.map?.j ? JSON.parse(rec.map.j) : null;
      if (!raw) continue;

      const out = await parseBlock(raw);

      for (const p of out.pools)   await emit(STREAM_OUTS.new_pool,  p);
      for (const s of out.swaps)   await emit(STREAM_OUTS.swap,      s);
      for (const l of out.liqs)    await emit(STREAM_OUTS.liquidity, l);

    } catch (e) {
      warn('parse error', e?.message || e);
    }
  }
  await ackMany(ids);
}

async function main() {
  await redisConnect();
  await createConsumerGroup({ redis, stream: STREAM_IN, group: GROUP });
  info('processor ready, reading…');

  await readLoop({
    redis,
    stream: STREAM_IN,
    group: GROUP,
    consumer: CONSUMER,
    handler,
    batch: Number(process.env.PROCESSOR_BATCH || 50),
    blockMs: Number(process.env.PROCESSOR_BLOCK_MS || 5000)
  });
}

main().catch(e => { err(e); process.exit(1); });
