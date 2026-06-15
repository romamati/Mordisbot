import { Kafka } from 'kafkajs';
import pg from 'pg';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:19092').split(',');
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://mordisbot:mordisbot@localhost:5432/mordisbot';
const WINDOW_SECONDS = Number(process.env.TREND_WINDOW_SECONDS || 60);
const THRESHOLD = Number(process.env.TREND_THRESHOLD || 5);
const IN_TOPIC = 'user.intent.parsed';
const OUT_TOPIC = 'trend.detected';
const GROUP_ID = 'cep-group';

const kafka = new Kafka({ clientId: 'cep-service', brokers: BROKERS });
const consumer = kafka.consumer({ groupId: GROUP_ID });
const producer = kafka.producer();
const pool = new pg.Pool({ connectionString: DATABASE_URL });

const windows = new Map(); // "cocina::zona" -> timestamps[]
const notified = new Set(); // "cocina::zona" ya disparadas en esta corrida

function keyFor(cocina, zona) {
  return `${cocina}::${zona}`;
}

function updateWindow(key, now) {
  const arr = windows.get(key) || [];
  arr.push(now);
  const cutoff = now - WINDOW_SECONDS * 1000;
  const filtered = arr.filter((t) => t > cutoff);
  windows.set(key, filtered);
  return filtered.length;
}

async function main() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: IN_TOPIC, fromBeginning: true });
  console.log(`[cep-service] conectado a ${BROKERS.join(',')} - escuchando "${IN_TOPIC}" (grupo ${GROUP_ID})`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const { cocina, zona } = event.slots;
      const now = Date.now();

      const key = keyFor(cocina, zona);
      const count = updateWindow(key, now);

      if (count >= THRESHOLD && !notified.has(key)) {
        notified.add(key);

        await pool.query(
          `INSERT INTO trends (cocina, zona, count, window_seconds, detected_at)
           VALUES ($1, $2, $3, $4, now())`,
          [cocina, zona, count, WINDOW_SECONDS]
        );

        const payload = {
          cocina,
          zona,
          count,
          window_seconds: WINDOW_SECONDS,
          ts: new Date().toISOString(),
        };

        await producer.send({
          topic: OUT_TOPIC,
          messages: [{ value: JSON.stringify(payload) }],
        });

        console.log(`[cep-service] TENDENCIA DETECTADA: ${cocina}/${zona} (${count} en ${WINDOW_SECONDS}s) -> trends + ${OUT_TOPIC}`, payload);
      }
    },
  });
}

main().catch((err) => {
  console.error('[cep-service] error:', err);
  process.exit(1);
});
