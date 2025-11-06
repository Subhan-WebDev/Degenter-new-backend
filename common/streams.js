// common/streams.js
import { setTimeout as sleep } from 'node:timers/promises';
import { info, warn, debug } from './log.js';

/**
 * createConsumerGroup({ redis, stream, group })
 */
export async function createConsumerGroup({ redis, stream, group }) {
  try {
    await redis.xGroupCreate(stream, group, '$', { MKSTREAM: true });
    info('[xgroup] created', { stream, group });
  } catch (e) {
    if (!String(e.message).includes('BUSYGROUP')) throw e;
    info('[xgroup] exists', { stream, group });
  }
}

/**
 * readLoop({ redis, stream, group, consumer, handler, batch=100, blockMs=5000 })
 * Calls handler(records) with an array of { id, map } per batch.
 * handler must ack each id or you can autoAck=true to ack after success.
 */
export async function readLoop({
  redis, stream, group, consumer,
  handler,
  batch = 100,
  blockMs = 5_000,
  autoAck = false,
}) {
  info('[xread] starting', { stream, group, consumer, batch, blockMs });

  while (true) {
    try {
      const res = await redis.xReadGroup(group, consumer, { key: stream, id: '>' }, { COUNT: batch, BLOCK: blockMs });
      if (!res) continue; // timeout
      for (const { name, messages } of res) {
        debug('[xread] batch', name, messages.length);
        // normalize messages into { id, map }
        const records = messages.map(m => ({
          id: m.id,
          map: Object.fromEntries(m.message ? Object.entries(m.message) : []),
        }));
        await handler(records, {
          ack: async (id) => redis.xAck(stream, group, id),
          ackMany: async (ids) => ids.length && redis.xAck(stream, group, ids),
        });
        if (autoAck) {
          const ids = records.map(r => r.id);
          await redis.xAck(stream, group, ids);
        }
      }
    } catch (e) {
      warn('[xread] error', e?.message || e);
      await sleep(1000);
    }
  }
}

/**
 * claimStale({
 *   redis, stream, group, consumer,
 *   minIdleMs=60000, count=100
 * })
 * Returns array of IDs claimed to this consumer.
 */
export async function claimStale({ redis, stream, group, consumer, minIdleMs = 60_000, count = 100 }) {
  const pending = await redis.xPending(stream, group);
  if (!pending || !pending.count) return [];
  const low = pending.minIdle || minIdleMs;
  const list = await redis.xPendingRange(stream, group, '-', '+', Math.min(count, pending.count));
  const stale = list.filter(x => x.idle >= low).map(x => x.id);
  if (!stale.length) return [];
  await redis.xClaim(stream, group, consumer, minIdleMs, stale);
  return stale;
}
