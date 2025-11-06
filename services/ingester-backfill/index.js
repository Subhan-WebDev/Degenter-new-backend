// services/ingester-backfill/index.js
import 'dotenv/config';
import '../../common/load-env.js';  // must be first import

process.env.SVC_NAME = process.env.SVC_NAME || 'ingester-backfill';

import { createRedisClient } from '../../common/redis-client.js';
import { getBlock, getBlockResults, unwrapBlock, unwrapBlockResults } from '../../common/rpc.js';
import { info, warn, err } from '../../common/log.js';

const STREAM = process.env.STREAM_RAW || 'chain:raw_blocks';

const START = Number(process.env.BACKFILL_START || 0);
const END   = Number(process.env.BACKFILL_END   || 0);
const BATCH = Number(process.env.BACKFILL_BATCH || 25);
const SLEEP = Number(process.env.BACKFILL_SLEEP_MS || 75);

if (!START || !END || END < START) {
  console.error('Set BACKFILL_START and BACKFILL_END envs (END >= START).');
  process.exit(1);
}

const { client: redis, connect: redisConnect } = createRedisClient('backfill');

async function xaddRawBlock(payload) {
  return redis.xAdd(STREAM, '*', { j: JSON.stringify(payload) });
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function main() {
  await redisConnect();
  info('connected to redis');

  for (let h = START; h <= END; h += BATCH) {
    const upto = Math.min(h + BATCH - 1, END);
    info(`fetching heights ${h}..${upto}`);

    const heights = [];
    for (let k = h; k <= upto; k++) heights.push(k);

    for (const height of heights) {
      try {
        const [b, r] = await Promise.all([ getBlock(height), getBlockResults(height) ]);
        const blk = unwrapBlock(b);
        const res = unwrapBlockResults(r);
        if (!blk?.header) { warn('no block', height); continue; }

        const payload = {
          height: Number(blk.header.height),
          time: blk.header.time,
          header: blk.header,
          txs: blk.txs,
          results: res.txs_results
        };
        await xaddRawBlock(payload);
      } catch (e) {
        warn('height error', height, e?.message || e);
      }
      if (SLEEP) await sleep(SLEEP);
    }
  }
  info('backfill complete');
}

main().catch(e => { err(e); process.exit(1); });
