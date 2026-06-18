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

Requiere Docker corriendo. Primero crear el `.env` (gitignoreado) a
partir del ejemplo:

```bash
cp .env.example .env
```

Levantar todo el stack (broker, db, esp-service, cep-service,
b2b-dashboard):

```bash
docker compose up -d --build
```

`intake-simulator` es un script que corre una vez y termina (aparece
como `Exited (0)` en `docker compose ps`: es lo esperado). Para
disparar la tanda de eventos de la demo en vivo:

```bash
docker compose run --rm intake-simulator
```

Abrir `http://localhost:3000` y ver el dashboard actualizarse en
~25-30s. Para repetir la demo, volver a correr el `docker compose run`.

> Debug fuera de docker: `intake-simulator` publica con
> `KAFKA_BROKERS=localhost:19092 npm start` (desde
> `services/intake-simulator`). Los servicios con DB (`esp/cep/b2b`)
> esperan un Postgres en `localhost:5432`; dentro de la red de compose
> usan `broker:9092` y `db:5432` via `.env`.

## Datos de prueba

`services/intake-simulator/data/sample-events.json` incluye una tanda
de eventos donde `cocina=sushi, zona=Palermo` aparece 6 veces en menos
de 30 segundos (suficiente para disparar la regla con el umbral
default de 5 en 60s), mezclada con otras combinaciones que NO la
disparan (para mostrar que la regla es selectiva).

## Resultado verificado

Corrida real del flujo completo (`docker compose up -d --build` +
`docker compose run --rm intake-simulator`) con
`services/intake-simulator/data/sample-events.json`:

**`esp-service`** cuenta cada `(cocina, zona)` en la ventana deslizante;
`sushi/Palermo` crece hasta 6 y el resto queda por debajo del umbral:

```
[esp-service] sushi/Palermo -> count=1 (ventana 60s)
[esp-service] pizza/Belgrano -> count=1 (ventana 60s)
[esp-service] sushi/Palermo -> count=2 (ventana 60s)
[esp-service] parrilla/Caballito -> count=1 (ventana 60s)
[esp-service] sushi/Palermo -> count=3 (ventana 60s)
[esp-service] sushi/Palermo -> count=4 (ventana 60s)
[esp-service] pizza/Belgrano -> count=2 (ventana 60s)
[esp-service] sushi/Palermo -> count=5 (ventana 60s)
[esp-service] sushi/Palermo -> count=6 (ventana 60s)
[esp-service] vegetariana/Almagro -> count=1 (ventana 60s)
```

**`cep-service`** dispara al CRUZAR el umbral (5to evento), inserta en
`trends` y publica en `trend.detected`:

```
[cep-service] TENDENCIA DETECTADA: sushi/Palermo (5 en 60s) -> trends + trend.detected {
  cocina: 'sushi',
  zona: 'Palermo',
  count: 5,
  window_seconds: 60,
  ts: '2026-06-15T15:43:04.082Z'
}
```

Reporta `count=5` (no 6) porque la regla salta en el instante del cruce
del umbral y no vuelve a dispararse; `esp_counts`, en cambio, cuenta
todos los eventos y llega a 6. Ninguna otra combinacion
(`pizza/Belgrano`, etc.) supera el umbral, asi que no genera tendencia
(la regla es selectiva).

**`b2b-dashboard`** (`http://localhost:3000`): con `trends` vacio muestra
"Sin tendencias todavia"; despues del flujo aparece la fila
`sushi | Palermo | 5 | 60s` con su timestamp, y la pagina se autorefresca
cada 5s.

## Deploy (Railway)

El endpoint B2B está deployado en Railway con datos de ejemplo:

```
GET https://mordisbot-production.up.railway.app/api/trends
```

Respuesta esperada:

```json
{
  "count": 3,
  "trends": [
    { "cocina": "sushi",    "zona": "Palermo",   "count": 6, "window_seconds": 60 },
    { "cocina": "pizza",    "zona": "Belgrano",  "count": 3, "window_seconds": 60 },
    { "cocina": "parrilla", "zona": "Caballito", "count": 2, "window_seconds": 60 }
  ]
}
```

El deploy muestra el producto B2B (endpoint de tendencias) con datos de ejemplo.

El flujo completo con Kafka (intake-simulator → esp-service → cep-service → b2b-dashboard)
se demuestra localmente con `docker compose up` — ver sección "Como correr" arriba.

## Checklist

- [x] `docker compose up` levanta broker + db
- [x] `intake-simulator` publica eventos de prueba
- [x] `esp-service` consume y agrega por (cocina, zona)
- [x] `cep-service` detecta el patron, INSERT en `trends` + publica `trend.detected`
- [x] `b2b-dashboard` muestra las tendencias
- [x] Integracion completa en `docker-compose.yml` (flujo E2E verificado)
- [x] README completo (resultados reales)
- [x] White paper — ver whitepaper/mordisbot-whitepaper-v4.pdf
- [x] Deploy en Railway — https://mordisbot-production.up.railway.app/api/trends

## Stack

- **Broker**: Redpanda (API Kafka) - ver comparativa Kafka vs Redpanda
  en el white paper (`whitepaper/README.md`)
- **Servicios**: Node.js + kafkajs
- **Base de datos**: Postgres (imagen `pgvector/pgvector`)
- **Orquestacion**: Docker Compose
