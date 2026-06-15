# Tareas - Mordisbot TPIF

Cada tarea trae especificacion completa, criterios de aceptacion y
como probarla. Referencias a `01-CONTEXTO-PROYECTO.md` (el "por que")
y `02-ARQUITECTURA-POC.md` (esquemas, env vars, comparativas) cuando
hace falta mas detalle.

## Orden y dependencias

- **Tareas 1, 2 y 3** (esp-service, cep-service, b2b-dashboard) son
  INDEPENDIENTES entre si - se pueden hacer en cualquier orden, o en
  paralelo (carpetas distintas, sin conflictos de git).
- **Tarea 4** (integrar docker-compose) requiere 1-3 terminadas.
- **Tarea 5** (E2E + README) requiere 4.
- **Tarea 6** (white paper: resumen + funcional) puede hacerse en
  paralelo con 1-5.
- **Tarea 7** (white paper: eje tecnico) requiere 5 (describe lo
  IMPLEMENTADO, no un plan teorico).
- **Tarea 8** (ensamblar white paper) requiere 6 y 7.
- **Tareas 9 y 10** son opcionales/stretch, solo si sobra tiempo
  despues de 1-8.

## Prioridad para la entrega del 17/06

Imprescindible: Tareas 1-8. Opcional: 9-10.

---

## Tarea 1 - Implementar `esp-service`

**Estado:** [x] hecha

**Objetivo:** consumer ESP que agrega conteos por (cocina, zona) en
ventana deslizante y los persiste en `esp_counts`.

**Contexto:** ver `02-ARQUITECTURA-POC.md` (topic `user.intent.parsed`,
tabla `esp_counts`, variable `TREND_WINDOW_SECONDS`). El patron de
conexion a Kafka es el mismo que usa `services/intake-simulator/index.js`
(ya implementado), solo que aca es un `consumer` en vez de `producer`.

**Archivos a crear:**
- `services/esp-service/package.json`
- `services/esp-service/Dockerfile`
- `services/esp-service/index.js`

**Especificacion:**

`package.json`:
```json
{
  "name": "esp-service",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "kafkajs": "^2.2.4",
    "pg": "^8.11.0"
  }
}
```

`Dockerfile` (igual al de `intake-simulator`):
```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
CMD ["npm", "start"]
```

`index.js`:
```javascript
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
```

**Criterios de aceptacion:**

- [x] El servicio levanta sin errores
- [x] Al consumir `services/intake-simulator/data/sample-events.json`,
      se ven logs `[esp-service] sushi/Palermo -> count=N` con N
      llegando a 6
- [x] La tabla `esp_counts` tiene filas para cada `(cocina, zona)` que
      aparecio en los datos de prueba

**Como probar:**

```bash
docker compose up -d broker db
cd services/esp-service && npm install
KAFKA_BROKERS=localhost:19092 DATABASE_URL=postgresql://mordisbot:mordisbot@localhost:5432/mordisbot npm start
```

En otra terminal:
```bash
cd services/intake-simulator && npm install
KAFKA_BROKERS=localhost:19092 npm start
```

Verificar `esp_counts`:
```bash
docker exec -it mordisbot-db psql -U mordisbot -d mordisbot -c "SELECT * FROM esp_counts;"
```

---

## Tarea 2 - Implementar `cep-service`

**Estado:** [x] hecha

**Objetivo:** consumer CEP que detecta la regla "Tendencia detectada"
(escalada, ver `02-ARQUITECTURA-POC.md`) y dispara la accion de
negocio.

**Archivos a crear:**
- `services/cep-service/package.json`
- `services/cep-service/Dockerfile`
- `services/cep-service/index.js`

**Especificacion:**

`package.json`: igual estructura que Tarea 1, `"name": "cep-service"`,
mismas dependencias (`kafkajs`, `pg`).

`Dockerfile`: igual al de Tarea 1.

`index.js`:
```javascript
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
```

**Criterios de aceptacion:**

- [x] Al correr el flujo completo, aparece EXACTAMENTE 1 fila en
      `trends` para `sushi`/`Palermo`
- [x] En consola aparece `TENDENCIA DETECTADA: sushi/Palermo (5 en 60s)`
      (la regla dispara al CRUZAR el umbral: `count >= THRESHOLD`, o sea
      en el 5to evento; el `notified` evita re-disparos, por eso reporta
      5 y no 6 - a diferencia de `esp_counts`, que cuenta todos y llega a 6)
- [x] Ninguna otra combinacion (`pizza`/`Belgrano`, etc.) genera fila en
      `trends` (no llegan al umbral en `sample-events.json`)

**Como probar:** igual que Tarea 1, agregando:
```bash
docker exec -it mordisbot-db psql -U mordisbot -d mordisbot -c "SELECT * FROM trends;"
```

---

## Tarea 3 - Implementar `b2b-dashboard`

**Estado:** [ ] pendiente

**Objetivo:** mostrar las tendencias detectadas - el producto B2B de la
idea original (ver `01-CONTEXTO-PROYECTO.md`, seccion 1).

**Archivos a crear:**
- `services/b2b-dashboard/package.json`
- `services/b2b-dashboard/Dockerfile`
- `services/b2b-dashboard/index.js`

**Especificacion:**

`package.json`:
```json
{
  "name": "b2b-dashboard",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "express": "^4.19.2",
    "pg": "^8.11.0"
  }
}
```

`Dockerfile`:
```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
```

`index.js`:
```javascript
import express from 'express';
import pg from 'pg';

const PORT = Number(process.env.DASHBOARD_PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://mordisbot:mordisbot@localhost:5432/mordisbot';

const pool = new pg.Pool({ connectionString: DATABASE_URL });
const app = express();

app.get('/', async (req, res) => {
  const { rows } = await pool.query(
    'SELECT cocina, zona, count, window_seconds, detected_at FROM trends ORDER BY detected_at DESC LIMIT 20'
  );

  const rowsHtml = rows
    .map(
      (r) =>
        `<tr><td>${r.cocina}</td><td>${r.zona}</td><td>${r.count}</td><td>${r.window_seconds}s</td><td>${r.detected_at}</td></tr>`
    )
    .join('');

  res.send(`<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>Mordisbot - Tendencias (B2B)</title>
  <style>
    body { font-family: sans-serif; margin: 2rem; }
    table { border-collapse: collapse; }
    th, td { border: 1px solid #ccc; padding: 0.5rem 1rem; text-align: left; }
    th { background: #f0f0f0; }
  </style>
</head>
<body>
  <h1>Tendencias detectadas (producto B2B)</h1>
  <p>${rows.length} tendencia(s) detectada(s). Se refresca cada 5s.</p>
  <table>
    <tr><th>Cocina</th><th>Zona</th><th>Conteo</th><th>Ventana</th><th>Detectado</th></tr>
    ${rowsHtml || '<tr><td colspan="5">Sin tendencias todavia</td></tr>'}
  </table>
</body>
</html>`);
});

app.listen(PORT, () => {
  console.log(`[b2b-dashboard] escuchando en http://localhost:${PORT}`);
});
```

**Criterios de aceptacion:**

- [ ] `http://localhost:3000` responde 200 y muestra la tabla
- [ ] Antes de correr el flujo, muestra "Sin tendencias todavia"
- [ ] Despues de correr el flujo completo, aparece la fila
      `sushi | Palermo`

**Como probar:**

```bash
cd services/b2b-dashboard && npm install
DASHBOARD_PORT=3000 DATABASE_URL=postgresql://mordisbot:mordisbot@localhost:5432/mordisbot npm start
```

Abrir `http://localhost:3000` antes y despues de correr
`intake-simulator`.

---

## Tarea 4 - Integrar todo en `docker-compose.yml`

**Estado:** [ ] pendiente

**Objetivo:** que `docker compose up -d --build` levante TODO (broker,
db, esp-service, cep-service, b2b-dashboard) y que `intake-simulator`
se pueda disparar on-demand para la demo.

**Precondicion:** Tareas 1-3 completas.

**Archivos a modificar:** `docker-compose.yml`

**Especificacion:**

Descomentar los 4 bloques de servicios propios (ya estan en el archivo
como referencia, comentados). Deben quedar asi:

```yaml
  intake-simulator:
    build: ./services/intake-simulator
    container_name: mordisbot-intake-simulator
    depends_on:
      - broker
    env_file: .env

  esp-service:
    build: ./services/esp-service
    container_name: mordisbot-esp-service
    depends_on:
      - broker
      - db
    env_file: .env

  cep-service:
    build: ./services/cep-service
    container_name: mordisbot-cep-service
    depends_on:
      - broker
      - db
    env_file: .env

  b2b-dashboard:
    build: ./services/b2b-dashboard
    container_name: mordisbot-b2b-dashboard
    depends_on:
      - db
    ports:
      - "3000:3000"
    env_file: .env
```

**Nota sobre `intake-simulator`:** es un script que corre una vez y
termina (exit 0) - es ESPERADO que `docker compose ps` lo muestre como
"Exited (0)" despues de `up`. Para la demo en vivo conviene:

1. `docker compose up -d broker db esp-service cep-service b2b-dashboard`
2. Abrir `http://localhost:3000` (deberia estar vacio)
3. `docker compose run --rm intake-simulator` <- esto "dispara" la
   tanda de eventos en vivo
4. Ver el dashboard actualizarse en ~25-30s

**Criterios de aceptacion:**

- [ ] `docker compose up -d --build` levanta `broker`, `db`,
      `esp-service`, `cep-service`, `b2b-dashboard` sin errores
- [ ] `docker compose run --rm intake-simulator` corre y termina con
      exit code 0
- [ ] `docker compose logs esp-service` muestra lineas
      `[esp-service] sushi/Palermo -> count=N` con N creciendo hasta 6
- [ ] `docker compose logs cep-service` muestra la linea
      `TENDENCIA DETECTADA: sushi/Palermo`
- [ ] `http://localhost:3000` muestra la fila `sushi | Palermo`

---

## Tarea 5 - Test end-to-end y completar README

**Estado:** [ ] pendiente

**Objetivo:** dejar evidencia real (no solo "esperado") de que el
flujo funciona, y actualizar el checklist general.

**Precondicion:** Tarea 4 completa.

**Archivos a modificar:** `README.md` (raiz)

**Especificacion:**

1. Correr el flujo completo (Tarea 4, pasos 1-4).
2. En `README.md`, reemplazar la seccion "Resultado esperado" por
   "Resultado verificado" con:
   - Las lineas de log reales de `esp-service` y `cep-service`
     (copiar/pegar, se pueden recortar timestamps si molestan).
   - Una descripcion de lo que se ve en `http://localhost:3000`.
3. Actualizar el checklist: marcar `[x]` los items que ya estan hechos
   (intake-simulator, esp-service, cep-service, b2b-dashboard,
   integracion).

**Criterios de aceptacion:**

- [ ] `README.md` tiene evidencia real (logs/descripcion) del flujo
      funcionando
- [ ] Checklist actualizado

---

## Tarea 6 - White paper: Resumen ejecutivo + Eje Funcional

**Estado:** [ ] pendiente

**Objetivo:** redactar las secciones 1 y 2 del white paper (ver
`whitepaper/README.md` para la estructura completa de 5 paginas).

**Archivos a crear:** `whitepaper/01-resumen-y-funcional.md`

**Especificacion:**

### Seccion 1 - Problema y producto (resumen ejecutivo, ~150-200 palabras)

Version corta de `01-CONTEXTO-PROYECTO.md` seccion 1: que es Mordisbot,
que problema resuelve, modelo de negocio (prioridad paga + venta de
tendencias B2B). Reescribir con palabras propias, no copiar.

### Seccion 2 - Eje Funcional (~700-900 palabras)

- **2.1 Concepto** (~200 palabras): pub-sub + procesamiento de eventos
  (ESP/CEP) explicado en terminos de negocio - por que un sistema
  "reactivo a eventos" resuelve mejor el problema que uno
  sincronico/monolitico. Basarse en `01-CONTEXTO-PROYECTO.md`,
  secciones 2.6 y 2.7.

- **2.2 Ejemplos de utilidad** (~200 palabras): elegir 2-3 reglas CEP
  de la tabla en `01-CONTEXTO-PROYECTO.md` 2.7 (sugerido: Tendencia
  detectada, Alerta de coste, Restaurante caliente) y explicar el
  beneficio de negocio de cada una.

- **2.3 Comparativa de alternativas** (~300-400 palabras): usar
  `02-ARQUITECTURA-POC.md`, seccion "Comparativas para el white
  paper" - Kafka vs RabbitMQ (decision de categoria) y Kafka vs
  Redpanda (decision de implementacion), explicando como cada decision
  beneficia al negocio (time-to-market, fan-out para el producto B2B
  sin tocar el pipeline, control de costo via CEP).

**Criterios de aceptacion:**

- [ ] Archivo creado, ~850-1100 palabras totales
- [ ] No reproduce el texto de la Actividad 1 palabra por palabra
      (esta reescrito/condensado)

---

## Tarea 7 - White paper: Eje Tecnico

**Estado:** [ ] pendiente

**Precondicion:** Tareas 1-5 completas (se describe lo IMPLEMENTADO,
no un plan teorico).

**Objetivo:** redactar la seccion 3 del white paper.

**Archivos a crear:** `whitepaper/02-eje-tecnico.md`

**Especificacion:**

- **3.1 Investigacion de Kafka** (~400-500 palabras): modelo de log
  distribuido (topics, particiones, offsets), consumer groups,
  garantias at-least-once, retencion y replay.

- **3.2 Mapeo a la arquitectura de Mordisbot** (~200-300 palabras):
  tabla de `02-ARQUITECTURA-POC.md` ("Mapeo completo de topics
  Mordisbot -> Kafka") con los 6 topics y partition keys sugeridas.

- **3.3 PoC: caso de uso "Tendencia detectada"** (~300-400 palabras):
  describir el flujo implementado (intake-simulator -> esp-service /
  cep-service -> trends -> b2b-dashboard), referenciar
  `docs/poc-architecture.svg`, explicar el umbral de demo vs la regla
  real (tabla en `02-ARQUITECTURA-POC.md`).

**Criterios de aceptacion:**

- [ ] Archivo creado, ~900-1200 palabras totales
- [ ] La descripcion de 3.3 coincide con lo que el codigo REALMENTE
      hace (no con un plan teorico)

---

## Tarea 8 - White paper: Riesgos + ensamblar documento final

**Estado:** [ ] pendiente

**Precondicion:** Tareas 6 y 7 completas.

**Objetivo:** redactar la seccion 4 y armar el documento final.

**Archivos a crear:**
- `whitepaper/03-riesgos-y-anexos.md`
- `whitepaper/whitepaper.md`

**Especificacion:**

### `03-riesgos-y-anexos.md`

- **Seccion 4 - Riesgos y proximos pasos** (~150-200 palabras):
  condensar `01-CONTEXTO-PROYECTO.md` 2.9 + agregar 1-2 aprendizajes de
  implementar Kafka/Redpanda (ej. operar particiones/consumer groups en
  produccion, Redpanda Cloud vs self-host para el deploy).
- Lista de anexos: diagramas completos de Actividad 1, diagrama de
  esta PoC (`docs/poc-architecture.svg`), tabla de stack actualizada
  (agregar fila "Broker pub-sub -> Kafka/Redpanda" a la tabla de
  `01-CONTEXTO-PROYECTO.md` 2.8).

### `whitepaper.md`

Concatenar, en orden: portada minima (titulo, equipo, fecha) + indice +
contenido de `01-resumen-y-funcional.md` + `02-eje-tecnico.md` +
`03-riesgos-y-anexos.md`.

**Criterios de aceptacion:**

- [ ] `whitepaper/whitepaper.md` existe y es coherente de punta a punta
- [ ] El cuerpo (sin portada/indice/anexos) tiene ~2000-2500 palabras
      (~5 paginas con formato normal)

---

## Tarea 9 (opcional / stretch) - Segunda regla CEP: "Preferencia personal"

**Estado:** [ ] pendiente

**Objetivo:** demostrar que `cep-service` soporta reglas declarativas
(no hardcodeadas), implementando una version escalada de la Regla 3 de
Actividad 1 ("Preferencia personal", ver `01-CONTEXTO-PROYECTO.md` 2.7).

**Archivos a modificar:**
- `services/cep-service/index.js`
- `.env.example`
- `db/init.sql`
- `services/intake-simulator/data/sample-events.json`

**Especificacion:**

1. Refactorizar `cep-service` para tener un array `rules`, donde cada
   regla define: clave de agrupacion, ventana, umbral y accion al
   disparar. La regla "Tendencia detectada" (Tarea 2) pasa a ser la
   primera entrada de ese array, sin cambiar su comportamiento.

2. Nueva regla "Preferencia personal" (escalada):
   - Ventana: nueva env `PERSONAL_WINDOW_SECONDS` (default `120`)
   - Clave de agrupacion: `chat_id::cocina` (no `cocina::zona`)
   - Umbral: nueva env `PERSONAL_THRESHOLD` (default `3`)
   - Condicion: el mismo `chat_id` pidio la misma `cocina` >=
     `PERSONAL_THRESHOLD` veces dentro de `PERSONAL_WINDOW_SECONDS`
   - Accion: `INSERT INTO personal_boosts (chat_id, cocina, count,
     detected_at) VALUES (...)` + log
     `[cep-service] BOOST PERSONAL: chat_id=C prefiere cocina=X (N veces)`

3. Agregar a `db/init.sql`:
```sql
CREATE TABLE IF NOT EXISTS personal_boosts (
    id          SERIAL PRIMARY KEY,
    chat_id     BIGINT NOT NULL,
    cocina      TEXT NOT NULL,
    count       INTEGER NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

4. Agregar a `.env.example`:
```
PERSONAL_WINDOW_SECONDS=120
PERSONAL_THRESHOLD=3
```

5. Para que la demo dispare esta regla, agregar al final de
   `services/intake-simulator/data/sample-events.json` 3 eventos con
   el mismo `chat_id` (ej. `2001`) y misma `cocina` (ej. `parrilla`),
   en distintas `zona`, separados por `delay_ms` chicos (ej. 2000).

**Criterios de aceptacion:**

- [ ] Con los eventos agregados, aparece la fila correspondiente en
      `personal_boosts` y el log `BOOST PERSONAL`
- [ ] La regla "Tendencia detectada" (Tarea 2) sigue funcionando igual

---

## Tarea 10 (opcional / stretch) - Guia de deploy en la nube

**Estado:** [ ] pendiente

**Objetivo:** documentar (y, si hay tiempo, ejecutar) el deploy del
mismo `docker-compose.yml` en una VM, para el item "proyecto deployado
en la nube" del enunciado (seccion 3.5 de `01-CONTEXTO-PROYECTO.md`).

**Archivos a crear:** `docs/04-DEPLOY.md`

**Especificacion:**

Documento con pasos para:
1. Provisionar una VM chica (opciones con free tier: Oracle Cloud
   Always Free, Fly.io, u otra equivalente - verificar condiciones
   vigentes al momento de hacerlo).
2. Instalar Docker + Docker Compose en la VM.
3. Clonar el repo (o copiar el proyecto) a la VM.
4. `cp .env.example .env`, ajustar si hace falta (ej. exponer
   `0.0.0.0` en vez de `localhost` para el dashboard).
5. `docker compose up -d --build`.
6. Abrir el puerto 3000 en el firewall/security group de la VM para
   acceder al dashboard desde afuera.
7. Notas de seguridad: no commitear `.env` con credenciales reales, no
   dejar puertos de `broker`/`db` expuestos publicamente sin necesidad.

**Criterios de aceptacion:**

- [ ] `docs/04-DEPLOY.md` existe con pasos concretos y verificables
- [ ] (si se ejecuta) URL publica del dashboard funcionando, agregada
      al README principal
