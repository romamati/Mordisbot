import { Kafka } from 'kafkajs';
import pg from 'pg';

const BROKERS = (process.env.KAFKA_BROKERS || 'localhost:19092').split(',');
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://mordisbot:mordisbot@localhost:5432/mordisbot';
const WINDOW_SECONDS = Number(process.env.TREND_WINDOW_SECONDS || 60);
const TOPIC = 'user.intent.parsed';
const GROUP_ID = 'esp-group';

const kafka = new Kafka({ clientId: 'esp-service', brokers: BROKERS });
const consumer = kafka.consumer({ groupId: GROUP_ID });
const pool = new pg.Pool({ connectionString: DATABASE_URL });

// estado en memoria: clave "cocina::zona" -> array de timestamps (epoch ms)
const windows = new Map();

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

function currentWindowStart(now) {
  const ms = WINDOW_SECONDS * 1000;
  return new Date(Math.floor(now / ms) * ms);
}

async function upsertCount(cocina, zona, windowStart, count) {
  await pool.query(
    `INSERT INTO esp_counts (cocina, zona, window_start, count, updated_at)
     VALUES ($1, $2, $3, $4, now())
     ON CONFLICT (cocina, zona, window_start)
     DO UPDATE SET count = EXCLUDED.count, updated_at = now()`,
    [cocina, zona, windowStart, count]
  );
}

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  console.log(`[esp-service] conectado a ${BROKERS.join(',')} - escuchando "${TOPIC}" (grupo ${GROUP_ID})`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const { cocina, zona } = event.slots;
      const now = Date.now();

      const key = keyFor(cocina, zona);
      const count = updateWindow(key, now);
      const windowStart = currentWindowStart(now);

      await upsertCount(cocina, zona, windowStart, count);

      console.log(`[esp-service] ${cocina}/${zona} -> count=${count} (ventana ${WINDOW_SECONDS}s)`);
    },
  });
}

main().catch((err) => {
  console.error('[esp-service] error:', err);
  process.exit(1);
});
