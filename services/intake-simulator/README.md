# intake-simulator

**Rol en la PoC:** reemplaza temporalmente los filtros 1-3 del pipeline
(Telegram Adapter, Sanitizacion, NLU) - en vez de procesar mensajes
reales de Telegram, publica directamente eventos `user.intent.parsed`
ya "parseados", usando datos de prueba. Asi la PoC se enfoca en
demostrar Kafka/ESP/CEP (el Eje Tecnico elegido) sin depender de LLMs.

**Topic de salida:** `user.intent.parsed`

```json
{
  "chat_id": 1001,
  "ts": "2026-06-15T10:00:00.000Z",
  "slots": { "cocina": "sushi", "zona": "Palermo" },
  "confidence": 0.92
}
```

**Estado:** implementado (`index.js`). Lee `data/sample-events.json`,
espera `delay_ms` entre cada evento y publica al topic, logueando cada
publish por consola - eso es lo que se va a ver en la demo.

**Datos de prueba:** `data/sample-events.json` ya viene armado con una
tanda donde `sushi`/`Palermo` aparece 6 veces en <30s (dispara la regla
de tendencia con el umbral default) y otras combinaciones de relleno
que NO la disparan.

## Correr local (fuera de docker)

```bash
npm install
KAFKA_BROKERS=localhost:19092 npm start
```

## Correr dentro de docker compose

Descomentar el bloque `intake-simulator` en `docker-compose.yml`
(usa `KAFKA_BROKERS=broker:9092` via `.env`).
