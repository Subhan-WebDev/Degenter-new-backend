// services/ingester-realtime/index.js
import 'dotenv/config';
import '../../common/load-env.js';  // must be first import
process.env.SVC_NAME = process.env.SVC_NAME || 'ingester-realtime';

import { createRedisClient } from '../../common/redis-client.js';
import { getStatus, getBlock, getBlockResults, unwrapStatus, unwrapBlock, unwrapBlockResults } from '../../common/rpc.js';
import { info, warn, err, debug } from '../../common/log.js';

const STREAM = process.env.STREAM_RAW || 'chain:raw_blocks';
const GROUP  = process.env.STREAM_GROUP || 'processor';
const POLL_MS = Number(process.env.REALTIME_POLL_MS || 1000);

const { client: redis, connect: redisConnect } = createRedisClient('realtime');

async function xaddRawBlock(payload) {
  // store minimal fields; payload is JSON string to keep values intact
  return redis.xAdd(STREAM, '*', { j: JSON.stringify(payload) });
}

async function main() {
  await redisConnect();
  info('connected to redis');

  let lastSeen = 0;

  // soft resume: remember last height by reading latest item in stream (optional)
  try {
    const tail = await redis.xRevRange(STREAM, '+', '-', { COUNT: 1 });
    if (tail?.length) {
      const last = tail[0][1].j && JSON.parse(tail[0][1].j);
      lastSeen = Number(last?.header?.height || last?.height || 0);
      info('resume from stream tail height', lastSeen);
    }
  } catch (e) {
    warn('resume scan failed', e?.message || e);
  }

  while (true) {
    try {
      const st = await getStatus();
      const tip = unwrapStatus(st);
      if (!tip) { await new Promise(r => setTimeout(r, POLL_MS)); continue; }

      // stream heights in order (handle small gaps)
      const start = Math.max( (lastSeen || (tip-1)), tip - 5 ); // allow small rewind
      for (let h = start + 1; h <= tip; h++) {
        const [b, r] = await Promise.all([ getBlock(h), getBlockResults(h) ]);
        const blk = unwrapBlock(b);
        const res = unwrapBlockResults(r);

        if (!blk?.header) { warn('no block header at', h); continue; }

        const payload = {
          height: Number(blk.header.height),
          time: blk.header.time,
          header: blk.header,
          txs: blk.txs,                // base64 txs array
          results: res.txs_results     // events + codes per tx
        };

        await xaddRawBlock(payload);
        lastSeen = Number(blk.header.height);
        info('XADD', STREAM, 'height', lastSeen);
      }
    } catch (e) {
      warn('realtime loop', e?.message || e);
      await new Promise(r => setTimeout(r, POLL_MS));
    }
  }
}

main().catch(e => { err(e); process.exit(1); });
