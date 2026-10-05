// Outbox relay: publishes committed outbox rows to Kafka, then deletes them.
//
// If Kafka is down, rows simply wait in the table. If the relay crashes after
// publishing but before deleting, those rows are published again on the next
// pass, so consumers must be idempotent (the payment worker is).
import { Partitioners } from 'kafkajs';
import { pool } from './db.ts';
import { kafka, ensureTopics } from './kafka.ts';
import { prom, serveMetrics } from './metrics.ts';

const publishedTotal = new prom.Counter({ name: 'outbox_published_total', help: 'Outbox rows published to Kafka' });
new prom.Gauge({
  name: 'outbox_backlog',
  help: 'Rows waiting in the outbox. Grows while Kafka is down.',
  async collect() { this.set(Number((await pool.query('SELECT count(*) FROM outbox')).rows[0].count)); },
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

serveMetrics();
await ensureTopics();
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
await producer.connect();
console.log('outbox relay running');

for (;;) {
  const client = await pool.connect();
  let published = 0;
  try {
    await client.query('BEGIN');
    // SKIP LOCKED lets several relays run side by side without sending a row twice.
    const { rows } = await client.query(
      'SELECT id, topic, key, payload FROM outbox ORDER BY id LIMIT 500 FOR UPDATE SKIP LOCKED',
    );
    if (rows.length) {
      const byTopic = Map.groupBy(rows, (r) => r.topic as string);
      await producer.sendBatch({
        topicMessages: [...byTopic].map(([topic, rs]) => ({
          topic, messages: rs.map((r) => ({ key: r.key, value: JSON.stringify(r.payload) })),
        })),
      });
      await client.query('DELETE FROM outbox WHERE id = ANY($1)', [rows.map((r) => r.id)]);
    }
    await client.query('COMMIT');
    published = rows.length;
    publishedTotal.inc(published);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('relay: publish failed, retrying', (err as Error).message);
    await sleep(1000);
  } finally {
    client.release();
  }
  if (!published) await sleep(50); // idle poll interval
}
