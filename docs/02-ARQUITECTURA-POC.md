# Arquitectura tecnica de la PoC - "Tendencia detectada"

Referencia tecnica para implementar y para escribir el white paper.
Diagrama: `docs/poc-architecture.svg`. Contexto de negocio completo:
`01-CONTEXTO-PROYECTO.md`.

## Servicios

| Servicio | Rol | Consume | Produce |
|---|---|---|---|
| `intake-simulator` | Simula filtros 1-3 (Telegram Adapter, Sanitizacion, NLU): publica intents ya "parseados" desde datos de prueba | `data/sample-events.json` | topic `user.intent.parsed` |
| `esp-service` | ESP: agregacion en ventana deslizante por (cocina, zona) | topic `user.intent.parsed` (group `esp-group`) | tabla `esp_counts` |
| `cep-service` | CEP: detecta la regla "Tendencia detectada" (escalada) | topic `user.intent.parsed` (group `cep-group`) | tabla `trends` + topic `trend.detected` |
| `b2b-dashboard` | Producto B2B: muestra tendencias | tabla `trends` | HTTP `:3000` |

## Topics de Kafka (PoC)

### `user.intent.parsed`

```json
{
  "chat_id": 1001,
  "ts": "2026-06-15T10:00:00.000Z",
  "slots": { "cocina": "sushi", "zona": "Palermo" },
  "confidence": 0.92
}
```

- Productor: `intake-simulator`
- Consumidores: `esp-service` (group `esp-group`), `cep-service`
  (group `cep-group`) - dos consumer groups independientes sobre el
  mismo topic = fan-out real.

### `trend.detected`

```json
{
  "cocina": "sushi",
  "zona": "Palermo",
  "count": 5,
  "window_seconds": 60,
  "ts": "2026-06-15T10:01:30.000Z"
}
```

- Productor: `cep-service`
- Consumidores: ninguno en esta PoC (existe para demostrar
  extensibilidad - el argumento de "fan-out natural" de la Actividad
  1, seccion 2.6).

## Esquema de Postgres (`db/init.sql`)

```sql
CREATE TABLE IF NOT EXISTS esp_counts (
    cocina        TEXT NOT NULL,
    zona          TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    count         INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cocina, zona, window_start)
);

CREATE TABLE IF NOT EXISTS trends (
    id              SERIAL PRIMARY KEY,
    cocina          TEXT NOT NULL,
    zona            TEXT NOT NULL,
    count           INTEGER NOT NULL,
    window_seconds  INTEGER NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`window_start` = inicio del bucket actual, calculado como
`floor(now_ms / (TREND_WINDOW_SECONDS*1000)) * (TREND_WINDOW_SECONDS*1000)`.
Es un bucket "tumbling" alineado a `TREND_WINDOW_SECONDS`, usado solo
como vidriera/persistencia; el estado real de la ventana deslizante
vive en memoria de cada servicio (ver `03-TAREAS.md`, Tarea 1).

## Variables de entorno (`.env`)

| Variable | Default | Usado por |
|---|---|---|
| `KAFKA_BROKERS` | `broker:9092` (docker) / `localhost:19092` (local) | todos los servicios Node |
| `DATABASE_URL` | `postgresql://mordisbot:mordisbot@db:5432/mordisbot` | esp-service, cep-service, b2b-dashboard |
| `TREND_WINDOW_SECONDS` | `60` | esp-service, cep-service |
| `TREND_THRESHOLD` | `5` | cep-service |
| `DASHBOARD_PORT` | `3000` | b2b-dashboard |

## Regla CEP: real vs PoC

| | Regla 2 real (Actividad 1) | Regla en la PoC |
|---|---|---|
| Ventana | 72h | `TREND_WINDOW_SECONDS` (60s) |
| Umbral | >= 50 | `TREND_THRESHOLD` (5) |
| Condicion | `COUNT(user.intent.parsed WHERE cocina=X AND zona=Y) >= 50` | igual, sobre (cocina, zona), ventana deslizante en memoria |
| Accion | Emitir senal a fuente B2B + influencers afines | `INSERT trends` + publish `trend.detected` |

`sample-events.json` esta armado para que `sushi`/`Palermo` llegue a 6
ocurrencias en <30s (cruza el umbral default de 5 en 60s); el resto de
combinaciones no llega al umbral.

## Mapeo completo de topics Mordisbot -> Kafka (para white paper, 3.2)

Los 6 topics de la Actividad 1 (seccion 2.5), pensados como topics de
Kafka reales con particionado sugerido (la PoC solo implementa
`user.intent.parsed` y `trend.detected`; el resto queda como diseno
para el white paper):

| Topic | Partition key sugerida | Por que |
|---|---|---|
| `user.message.in` | `chat_id` | mantiene orden de mensajes por conversacion |
| `user.intent.parsed` | `chat_id` | idem; permite reconstruir el historial de un usuario en orden |
| `reco.proposed` | `chat_id` | idem |
| `reco.delivered` | `chat_id` | idem |
| `feedback.received` | `rest_id` | el CEP agrega por restaurante (reglas 1 y 5); conviene que todo el feedback de un restaurante caiga en la misma particion |
| `restaurant.update` | `rest_id` | el indexador de Vector DB procesa por restaurante |

## Comparativas para el white paper (seccion 2.3 del Eje Funcional)

### Kafka vs RabbitMQ (decision de categoria, opcion 2 del enunciado)

- **Modelo**: Kafka = log distribuido con retencion/replay y orden por
  particion; RabbitMQ = colas con routing flexible (exchanges) y
  entrega orientada a "consumir y descartar".
- **Fan-out + replay**: Kafka permite que ESP y CEP (y mas adelante
  Analitica/Logger) lean el MISMO stream completo de forma
  independiente, cada uno con su propio offset. En RabbitMQ esto
  requeriria un exchange tipo fanout + una cola por consumidor, sin la
  garantia de "replay desde el principio" que da la retencion de
  Kafka.
- **Para Mordisbot**: las reglas CEP (especialmente "tendencia
  detectada" y "preferencia personal") necesitan mirar ventanas largas
  de historia (72h, 7d) - el modelo de log de Kafka es mas natural para
  esto que un modelo de colas.
- RabbitMQ gana en latencia minima por mensaje y simplicidad de
  routing complejo (ej. distintos tipos de notificacion por
  exchange/routing key) - no es el cuello de botella de Mordisbot.

### Kafka vs Redpanda (decision de implementacion)

- Mismo protocolo/API (topics, particiones, consumer groups, offsets,
  retencion) - todo lo que se investiga sobre "Kafka" aplica.
- Redpanda: un solo binario/container, sin Zookeeper/KRaft manual,
  arranque en segundos - menor "tiempo y recursos" (justamente uno de
  los puntos que pide la exposicion del 25/06).
- Para una PoC academica con plazos ajustados, Redpanda reduce el
  riesgo operativo sin sacrificar ninguno de los conceptos de Kafka que
  hay que demostrar.
