// services/worker-timescale/index.js
import '../../common/load-env.js';
process.env.SVC_NAME = process.env.SVC_NAME || 'worker-timescale';

import { createRedisClient } from '../../common/redis-client.js';
import { createConsumerGroup, readLoop } from '../../common/streams.js';
import { info, warn, err, debug } from '../../common/log.js';

import { handlePoolEvent } from './writers/pools.js';
import { handleSwapEvent, handleLiquidityEvent } from './writers/trades.js';
import { handlePriceSnapshot } from './writers/prices.js';

const STREAMS = {
  new_pool:  process.env.STREAM_NEW_POOL  || 'events:new_pool',
  swap:      process.env.STREAM_SWAP      || 'events:swap',
  liquidity: process.env.STREAM_LIQUIDITY || 'events:liquidity',
  price:     process.env.STREAM_PRICE     || 'events:price_tick'
};

const GROUP     = process.env.TIMESCALE_GROUP || 'timescale';
const BATCH     = Number(process.env.TIMESCALE_BATCH || 64);
const BLOCK_MS  = Number(process.env.TIMESCALE_BLOCK_MS || 5000);

const { client: redis, connect: redisConnect } = createRedisClient('ts-writer');

async function makeReader(stream, handler) {
  await createConsumerGroup({ redis, stream, group: GROUP });
  const consumer = `${process.env.SVC_NAME}-${stream}-${Math.random().toString(36).slice(2, 8)}`;
  info('timescale reader ready', { stream, group: GROUP, consumer });

  return readLoop({
    redis,
    stream,
    group: GROUP,
    consumer,
    batch: BATCH,
    blockMs: BLOCK_MS,
    handler: async (records, { ackMany }) => {
      const ids = [];
      for (const rec of records) {
        ids.push(rec.id);
        try {
          const obj = rec.map?.j ? JSON.parse(rec.map.j) : null;
          if (!obj) continue;
          await handler(obj);
        } catch (e) {
          warn(`[${stream}] handler`, e?.message || e);
        }
      }
      await ackMany(ids);
    }
  });
}

async function main() {
  await redisConnect();

  // Start readers (one loop per stream)
  await Promise.all([
    makeReader(STREAMS.new_pool,  handlePoolEvent),
    makeReader(STREAMS.swap,      handleSwapEvent),
    makeReader(STREAMS.liquidity, handleLiquidityEvent),
    makeReader(STREAMS.price,     handlePriceSnapshot)
  ]);

  info('worker-timescale running');
}

main().catch(e => { err(e); process.exit(1); });
