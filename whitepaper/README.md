# White paper - esqueleto (maximo 5 paginas, sin contar portada/indice/anexos)

La Actividad 1 ya cubre gran parte de esto - la idea es CONDENSAR, no
reescribir. [REUSAR] = sale casi directo de Actividad 1 (resumido).
[NUEVO] = hay que escribirlo para este TP.

## 1. Problema y producto (resumen ejecutivo) - ~0.4 pag

[REUSAR - condensar] Version corta de la idea original: que es
Mordisbot, que problema resuelve, modelo de negocio (prioridad paga +
venta de tendencias B2B).

## 2. Eje Funcional - ~1.6-2 pag

### 2.1 Concepto

[REUSAR] Pub-sub + procesamiento de eventos (ESP/CEP) explicado en
terminos de negocio: por que un sistema "reactivo a eventos" resuelve
mejor el problema que uno sincronico/monolitico.

### 2.2 Ejemplos de utilidad

[REUSAR - elegir 2-3] De la seccion 3 de Actividad 1: deteccion de
tendencias (B2B), alerta de costo, personalizacion por usuario, pausa
de prioridad paga por calidad.

### 2.3 Comparativa de alternativas

[NUEVO]
- **Categoria (Kafka vs RabbitMQ):** modelo de log vs modelo de colas
  - orden/replay/retencion (Kafka) vs simplicidad de routing y
  latencia minima por mensaje (RabbitMQ). Por que Kafka encaja mejor
  con "fan-out + replay para ESP/CEP".
- **Implementacion (Kafka vs Redpanda):** mismo modelo/API, Redpanda
  reduce la operacion (un solo binario, sin Zookeeper) - relevante
  para "tiempo y recursos" que pide la consigna para la exposicion.
- Como cada decision beneficia al negocio: time-to-market (Redpanda),
  capacidad de agregar el producto B2B sin tocar el pipeline (fan-out
  de Kafka), control de costo via CEP (regla 4 de Actividad 1).

## 3. Eje Tecnico - ~1.6-2 pag

### 3.1 Investigacion de Kafka

[NUEVO] Modelo de log distribuido: topics, particiones, offsets,
consumer groups, replicacion, garantias at-least-once,
retencion/replay. Esto es lo que hay que "investigar con nivel de
detalle" - vale la pena medio dia de doc oficial + algun video.

### 3.2 Mapeo a la arquitectura de Mordisbot

[REUSAR + ajustar] Los topics ya definidos en Actividad 1
(`user.message.in`, `user.intent.parsed`, `reco.proposed`,
`reco.delivered`, `feedback.received`, `restaurant.update`) como
topics de Kafka reales, con particiones/keys sugeridas (ej. particion
por `chat_id` para mantener orden por conversacion).

### 3.3 PoC: caso de uso "Tendencia detectada"

[NUEVO] Descripcion del flujo recortado (intake-simulator ->
esp-service / cep-service -> trends -> b2b-dashboard), diagrama (ver
`docs/poc-architecture.svg`), umbral usado para la demo y por que es
una version escalada de la Regla 2 real.

## 4. Riesgos y proximos pasos - ~0.4 pag

[REUSAR - condensar] De la seccion 5 de Actividad 1, + lo aprendido al
implementar Kafka (ej. operar particiones/consumer groups en
produccion, costo de Redpanda Cloud vs self-host).

## Anexos (no cuentan en las 5 paginas)

- Diagramas completos de Actividad 1 (pipeline, pub-sub, ESP/CEP)
- `docs/poc-architecture.svg`
- Tabla de stack tecnologico actualizada (agregar fila: Broker pub-sub
  -> Kafka/Redpanda)
