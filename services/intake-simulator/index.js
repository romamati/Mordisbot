import { Kafka } from 'kafkajs';
import { readFile } from 'fs/promises';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:19092').split(',');
const TOPIC = 'user.intent.parsed';

const kafka = new Kafka({ clientId: 'intake-simulator', brokers: BROKERS });
const producer = kafka.producer();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function main() {
  const raw = await readFile(new URL('./data/sample-events.json', import.meta.url));
  const events = JSON.parse(raw);

  await producer.connect();
  console.log(`[intake-simulator] conectado a ${BROKERS.join(',')} - publicando en "${TOPIC}"`);

  for (const ev of events) {
    await sleep(ev.delay_ms);

    const payload = {
      chat_id: ev.chat_id,
      ts: new Date().toISOString(),
      slots: ev.slots,
      confidence: ev.confidence,
    };

    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(payload) }],
    });

    console.log(`[intake-simulator] -> ${TOPIC}`, payload);
  }

  await producer.disconnect();
  console.log('[intake-simulator] listo.');
}

main().catch((err) => {
  console.error('[intake-simulator] error:', err);
  process.exit(1);
});
