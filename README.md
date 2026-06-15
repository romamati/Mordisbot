# Mordisbot - TPIF Ingenieria de Software II

**Equipo:** Matias Romanato (62072) - Tiziano Fuchinecco (64191)
**Catedra:** Emilio Rasic - 1er cuatrimestre 2026

## Que es esto

Continuacion de la Actividad 1 (arquitectura de Mordisbot). Para el TP
Integrador Final elegimos **Kafka** como Eje Tecnico: implementamos de
verdad el broker pub-sub que ya habiamos disenado, junto con una capa de
ESP (agregacion en ventana) y CEP (deteccion de patrones), sobre un caso
de uso recortado de la Regla CEP 2 ("Tendencia detectada") que alimenta
el producto B2B de venta de tendencias planteado en la idea original.

## Caso de uso de la PoC

1. `intake-simulator` simula busquedas de usuarios ya "parseadas"
   (`{cocina, zona, chat_id}`) y las publica en el topic Kafka
   `user.intent.parsed`. Reemplaza temporalmente los filtros 1-3 del
   pipeline (Telegram Adapter, Sanitizacion, NLU) para que la PoC se
   enfoque en el Eje Tecnico elegido.
2. `esp-service` consume ese topic (consumer group `esp-group`) y
   mantiene un conteo en ventana deslizante por `(cocina, zona)`.
3. `cep-service` consume el MISMO topic (consumer group `cep-group`,
   fan-out real) y aplica la regla: si `(cocina, zona)` supera
   `TREND_THRESHOLD` busquedas dentro de `TREND_WINDOW_SECONDS`,
   inserta una fila en `trends` y publica `trend.detected`.
4. `b2b-dashboard` muestra las tendencias detectadas - el producto B2B
   de la idea original, hecho tangible.

La regla real (Actividad 1) es ">= 50 busquedas en 72hs"; para la demo
usamos ">= 5 busquedas en 60s" (mismo patron, escala de demo).

## Arquitectura

Ver `docs/poc-architecture.svg`.

- **broker**: Redpanda, compatible con la API de Kafka (topics,
  particiones, consumer groups, retencion, at-least-once).
- **db**: Postgres + pgvector (consistente con el resto del proyecto).
- **intake-simulator -> broker -> {esp-service, cep-service}**: fan-out,
  el punto justificado en 2.2 de la Actividad 1.
- **cep-service -> db (trends) + broker (trend.detected)**
- **b2b-dashboard**: lee `trends`.

## Topics de Kafka

| Topic | Productor | Consumidores | Schema (resumen) |
|---|---|---|---|
| `user.intent.parsed` | intake-simulator | esp-service, cep-service | `{ chat_id, ts, slots: {cocina, zona}, confidence }` |
| `trend.detected` | cep-service | (futuros consumidores) | `{ cocina, zona, count, window_seconds, ts }` |

## Como correr

```bash
cp .env.example .env
docker compose up -d
```

Esto levanta:
- **broker**: Redpanda en `localhost:19092`
- **db**: Postgres en `localhost:5432` (con el esquema de `db/init.sql`
  ya aplicado)

A medida que se implementan los servicios propios, descomentar el
bloque correspondiente en `docker-compose.yml` y correr
`docker compose up -d --build` de nuevo.

### intake-simulator (ya implementado)

```bash
cd services/intake-simulator
npm install
KAFKA_BROKERS=localhost:19092 npm start
```

(o descomentar el servicio en `docker-compose.yml` para correrlo
dentro de la red de docker, usando `KAFKA_BROKERS=broker:9092`)

## Datos de prueba

`services/intake-simulator/data/sample-events.json` incluye una tanda
de eventos donde `cocina=sushi, zona=Palermo` aparece 6 veces en menos
de 30 segundos (suficiente para disparar la regla con el umbral
default de 5 en 60s), mezclada con otras combinaciones que NO la
disparan (para mostrar que la regla es selectiva).

## Resultado esperado

_(completar a medida que esp-service y cep-service esten listos)_

Al correr `intake-simulator` con los datos de prueba:

1. `esp-service` debe loguear el conteo de `(sushi, Palermo)`
   incrementando con cada evento.
2. Al alcanzar el umbral dentro de la ventana, `cep-service` debe:
   - insertar una fila en `trends` (Postgres)
   - publicar un evento en `trend.detected`
3. `b2b-dashboard` (`http://localhost:3000`) debe mostrar
   `sushi / Palermo` en la lista de tendencias.

## Checklist

- [x] `docker compose up` levanta broker + db
- [x] `intake-simulator` publica eventos de prueba
- [ ] `esp-service` consume y agrega por (cocina, zona)
- [ ] `cep-service` detecta el patron, INSERT en `trends` + publica `trend.detected`
- [ ] `b2b-dashboard` muestra las tendencias
- [ ] README completo (resultados reales, capturas)
- [ ] White paper (<= 5 paginas)
- [ ] (Stretch) Deploy en la nube

## Stack

- **Broker**: Redpanda (API Kafka) - ver comparativa Kafka vs Redpanda
  en el white paper (`whitepaper/README.md`)
- **Servicios**: Node.js + kafkajs
- **Base de datos**: Postgres (imagen `pgvector/pgvector`)
- **Orquestacion**: Docker Compose
