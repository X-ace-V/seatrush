import { Kafka, logLevel } from 'kafkajs';

export const kafka = new Kafka({
  clientId: 'seatrush',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
});

export const PAYMENT_REQUESTS = 'payment-requests';

// Idempotent: safe for every process to call on startup.
export async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  // Several replicas start at once and race to create it; losing that race is fine.
  await admin.createTopics({ topics: [{ topic: PAYMENT_REQUESTS, numPartitions: 6 }] })
    .catch((err) => { if (!/already exists/i.test(JSON.stringify(err.errors ?? err.message))) throw err; });
  await admin.disconnect();
}
