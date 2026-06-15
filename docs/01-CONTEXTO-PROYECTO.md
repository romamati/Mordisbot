# Contexto del proyecto - Mordisbot (TPIF Ingenieria de Software II)

> Este documento consolida TODO el contexto de negocio y arquitectura
> necesario para trabajar en este repo sin volver a los PDFs
> originales. Es la fuente de verdad del "por que" se hacen las cosas;
> el "que hay que hacer ahora" esta en `03-TAREAS.md`.

## Equipo y materia

- Ingenieria de Software II - catedra Emilio Rasic - 1er cuatrimestre 2026
- Matias Romanato (legajo 62072)
- Tiziano Fuchinecco (legajo 64191)

---

## 1. Idea original del producto (Mordisbot)

### Problema

En Argentina no existe una aplicacion que centralice de manera
actualizada la informacion sobre donde comer. Los buscadores actuales
ofrecen filtros muy rigidos que no se adaptan a lo que el usuario
quiere en el momento (tipo de comida, zona, presupuesto,
caracteristicas especificas del lugar).

### Producto: Mordisbot

Agente de inteligencia artificial que entiende lenguaje natural. El
usuario se comunica con el (por ejemplo via Telegram) y el agente,
usando informacion almacenada en una base de datos en Supabase, entrega
una lista curada con las opciones optimas.

Para garantizar veracidad de los datos, son los propios duenos de los
restaurantes quienes cargan la informacion de sus negocios (formulario
o interactuando con el agente). Al solicitar un sitio, el sistema
pondera las caracteristicas pedidas y exhibe los resultados.

### Modelo de negocio

1. **Prioridad paga**: restaurantes que quieran aparecer con mas
   frecuencia en las recomendaciones pagan una tarifa.
2. **Venta de tendencias de busqueda** (segunda fuente de ingresos, a
   evaluar): vender datos agregados sobre que se busca, donde y cuando
   (al propio sector gastronomico, por ejemplo).

### Stack y desarrollo (planteado originalmente)

- Vercel para hosting.
- Una IA especializada en analizar lenguaje natural para la toma de
  decisiones.
- Desarrollo publico en GitHub (historial de avances visible).

### Marketing y objetivo a largo plazo

- Alianzas con influencers del nicho gastronomico que muestren como
  usan Mordisbot.
- Objetivo: optimizar el algoritmo y captar la mayor cantidad de
  restaurantes posibles, ofreciendo variedad.

### Clientes y costos iniciales

- Dos tipos de clientes: usuarios (buscan donde comer) y duenos de
  locales (buscan ser sugeridos).
- Costos iniciales concentrados en: IA, hosting, base de datos,
  marketing de lanzamiento.

---

## 2. Resumen de "Actividad 1 - Arquitectura del sistema" (mayo 2026)

> Entrega previa del equipo. Define el pipeline interno, el estilo
> distribuido y el manejo de eventos de Mordisbot. Este TPIF
> IMPLEMENTA una porcion real de esta arquitectura.

### 2.1 Resumen ejecutivo (Actividad 1)

El procesamiento (no el almacenamiento) es el cuello de botella de
Mordisbot. Se apoya en una base vectorial (pgvector) para RAG, suma un
segundo modelo de IA para validar recomendaciones, y aprovecha un flujo
de eventos para alimentar el modelo de negocio (prioridad paga +
producto B2B de tendencias).

El sistema se modela como una cadena Pipes & Filters orquestada por un
broker pub-sub. Sobre ese bus se montan capas de Event Stream
Processing (agregaciones en tiempo real) y Complex Event Processing
(deteccion de patrones que dispara acciones de negocio).

### 2.2 Pipeline interno (Pipes & Filters)

| # | Filtro | Entrada -> Salida | Logica principal |
|---|---|---|---|
| 1 | Telegram Adapter (in) | TelegramUpdate -> RawMessage{chat,user,text,ts} | Webhook de Telegram, deduplica por update_id, normaliza payload |
| 2 | Sanitizacion + Auth | RawMessage -> AuthedMessage{role,profile} | Filtra emojis/comandos, identifica usuario o dueno, recupera perfil cacheado |
| 3 | NLU - LLM 1 | AuthedMessage -> Intent+Slots | LLM con prompt acotado, extrae zona, presupuesto, cocina, ambiente, restricciones, urgencia + score de confianza |
| 4 | RAG - recuperacion | Slots -> Top-K Candidates | Vectoriza la consulta, cosine search en pgvector con filtros duros (zona, presupuesto), top K=15 |
| 5 | Ranker de negocio | Top-K -> Ranked Top-3 | score = alfa*match + beta*prioridad_paga + gamma*NPS_movil + delta*recencia; beta tiene techo configurable |
| 6 | Validador - LLM 2 | Top-3+Slots -> ApprovedReco \| RejectReason | LLM mas chico verifica cumplimiento y horario; si rechaza, vuelve al ranker (loop corto) |
| 7 | Composer | ApprovedReco -> TelegramReply{markdown} | LLM 1 con prompt corto: genera mensaje, agrega 1-2 emojis, marca lo patrocinado |
| 8 | Telegram Adapter (out) | TelegramReply -> ack+signals | Envia respuesta y registra senales (clicks, reservas, tiempo hasta proxima consulta) |

Filtros 3, 6 y 7 usan LLMs; el resto es codigo deterministico.

### 2.3 Pseudocodigo del orquestador

```js
async function handleMessage(update) {
  const raw = telegramAdapter.in(update); // filtro 1
  const auth = sanitize(raw); // filtro 2
  const intent = await llm1.parse(auth); // filtro 3
  bus.publish('user.intent.parsed', intent);
  const candidates = await rag.topK(intent.slots, 15); // filtro 4
  const ranked = ranker.rank(candidates, intent); // filtro 5
  bus.publish('reco.proposed', ranked);
  let approved = await llm2.validate(ranked, intent); // filtro 6
  if (!approved.ok) { // loop corto
    const ranked2 = ranker.rerank(candidates, intent, approved.reason);
    approved = await llm2.validate(ranked2, intent);
  }
  const reply = await llm1.compose(approved); // filtro 7
  await telegramAdapter.out(reply); // filtro 8
  bus.publish('reco.delivered', { ...reply.meta, latency_ms: tEnd - tStart });
}
```

### 2.4 Por que Pipes & Filters

- Acoplamiento bajo: cada filtro depende solo de su contrato.
- Trazabilidad: eventos por etapa permiten medir latencia, costo y
  calidad por filtro.
- Optimizacion por etapa: el filtro 6 (validador) usa un modelo
  distinto y mas barato que el filtro 3 (NLU).
- Robustez: si el validador rechaza, un loop corto reintenta el ranker
  antes de devolver una respuesta degradada.

### 2.5 Estilo distribuido: Publish-Subscribe con broker

El broker entrega cada mensaje al menos una vez (at-least-once),
mantiene orden por topic y retiene 7 dias (replay/debugging).

| Topic | Schema (resumen) | Productor -> Consumidores |
|---|---|---|
| `user.message.in` | `{chat_id, user_id, text, ts}` | Adapter Telegram -> Orquestador, Logger |
| `user.intent.parsed` | `{chat_id, ts, slots:{zona,presup,cocina,ambiente,dieta}, confidence}` | Filtro NLU -> Analitica, CEP, Logger |
| `reco.proposed` | `{chat_id, ts, candidates:[{rest_id,score,score_breakdown:{a,b,g,d}}]}` | Ranker -> Validador, Analitica, Logger |
| `reco.delivered` | `{chat_id, ts, rest_ids:[int], positions:[int], paid_flags:[bool], latency_ms:int}` | Composer -> CEP, Analitica |
| `feedback.received` | `{chat_id, ts, rest_id, action:'visited'\|'reserved'\|'nps'\|'ignored', value:any}` | Bot NPS/Adapter -> CEP, Analitica, Ranker |
| `restaurant.update` | `{rest_id, ts, fields_changed:[string], source:'form'\|'bot', updated_by:int}` | Onboarding -> Indexador (Vector DB), Logger |

### 2.6 Por que Pub-Sub

- Fan-out natural: un evento (ej. `reco.delivered`) alimenta analitica,
  CEP y logger sin que el productor lo sepa.
- Resiliencia: si la analitica cae, el pipeline sigue funcionando; los
  eventos se acumulan y se consumen al volver.
- Escalabilidad horizontal: cada consumidor puede correr en replicas.
- El indexador del Vector DB es solo otro consumidor de
  `restaurant.update`.

### 2.7 ESP + CEP

**ESP** (ventanas de 1m, 1h, 24h) alimenta dashboard y al motor CEP:

- MAU/DAU por ciudad.
- Busquedas por cocina y zona (input directo del producto B2B de
  tendencias).
- NPS movil por restaurante (24h y 7d).
- % de busquedas que terminan en visita o reserva.
- Latencia p50/p95 y costo de IA por conversacion.

**CEP** correlaciona eventos en una ventana y dispara acciones:

| # | Nombre | Patron (ventana) | Accion disparada |
|---|---|---|---|
| 1 | Calidad cae | WINDOW 24h: COUNT(feedback.received WHERE action='nps' AND value<5 AND rest_id=R) >= 5 | Pausar prioridad paga de R, notificar al dueno |
| 2 | Tendencia detectada | WINDOW 72h: COUNT(user.intent.parsed WHERE cocina=X AND zona=Y) >= 50 | Emitir senal a la fuente B2B (tendencias) + influencers afines |
| 3 | Preferencia personal | WINDOW 7d: COUNT(user.intent.parsed WHERE chat_id=C AND cocina=X) >= 3 | Inyectar boost contextual de cocina X en el ranker para chat_id C |
| 4 | Alerta de coste | WINDOW 1h: SUM(cost_tokens) projected_to_day > daily_budget | Switch a modelo mas chico para NLU, degradar composer a plantillas |
| 5 | Restaurante caliente | WINDOW 24h: COUNT(feedback.received WHERE action IN (visited,reserved) AND rest_id=R) >= 10 | Marcar "tendencia" en feed B2B, aumentar gamma (peso NPS) por 48h |

Por que ESP + CEP y no solo procesamiento simple de eventos: las
decisiones mas importantes (pausar prioridad paga, detectar
tendencias, personalizar) surgen de la CORRELACION de varios eventos
en el tiempo, no de uno solo.

### 2.8 Stack tecnologico propuesto (Actividad 1)

| Pieza | Tecnologia elegida | Por que |
|---|---|---|
| Hosting de filtros | Vercel + Supabase Edge Functions | Ya esta en el plan, escala a demanda, sin admin de servidores |
| Broker pub-sub | Supabase Realtime / Redis Streams | Supabase ya esta en el stack; Redis Streams si se necesita retencion/replay |
| BD vectorial (RAG) | Supabase + pgvector | Misma base, cosine search y filtros SQL en la misma query |
| LLM 1 (NLU + Composer) | Modelo grande (ej. Claude Sonnet) | Comprension de espanol rioplatense y matices (urgencia, ambiente) |
| LLM 2 (Validador) | Modelo mas chico (ej. Haiku) | Tarea acotada y verificable, mas barato |
| ESP / CEP | Edge Functions con reglas declarativas + Materialized Views | Empezar simple en SQL/JS; migrar a ksqlDB o Flink si crece |
| Observabilidad | Logs en Supabase + dashboard propio | Mismo stack; despues se puede enchufar Grafana |

> Este TPIF "adelanta" la fila de ESP/CEP: en vez de reglas
> declarativas sobre Supabase, se implementa con Kafka (via Redpanda)
> como motor de eventos real - ver seccion 4.

### 2.9 Riesgos identificados (Actividad 1)

- Latencia agregada por 2 llamadas a LLM -> validador chico y en
  paralelo, reintento unico.
- Costo variable de IA -> regla CEP 4 degrada a modelo chico o
  plantillas.
- Conflicto prioridad paga vs calidad -> techo en beta del ranker +
  regla CEP 1.
- Inconsistencias en datos de restaurantes -> validacion de esquema en
  el indexador antes de embeddear.

---

## 3. Enunciado del TPIF - que pide la catedra

### 3.1 Cronograma

- 04/06: Enunciado TP y repaso Parcial
- 11/06: Parcial 2
- **17/06: Trabajo en clase y Entrega de TP (al finalizar la clase)**
- 25/06: Defensa TP + Recuperatorios

Los grupos y la idea de producto se mantienen del primer tramo del
cuatrimestre (Mordisbot).

### 3.2 Objetivo general

- Investigar e implementar una tecnologia en una prueba de concepto
  (PoC).
- Presentar un tema de los propuestos por la catedra.
- Enfoque "Eje funcional" (toma de decisiones orientada al negocio) +
  "Eje tecnico" (atributos de calidad).

### 3.3 Eje Tecnico

Opciones del enunciado:
1. Airflow o Prefect (dataflow, orquestacion de datos)
2. **Kafka o RabbitMQ (sistemas distribuidos, pub/sub)** <- elegido
3. N8N (eventos, generico) o Flink (single event processing)
4. Otros (previa validacion)

Debe incluir: explicacion de la herramienta/tecnologia con nivel de
detalle que muestre investigacion real, y una PoC con un caso de uso /
historia de usuario NO TRIVIAL.

### 3.4 Eje Funcional

Debe incluir: explicacion del concepto, ejemplos que muestren la
utilidad del Eje Tecnico elegido, y una comparativa entre alternativas
(pros/contras) explicando como las decisiones tecnicas benefician una
o varias necesidades de negocio del producto.

### 3.5 Entregables (17/06)

- **White paper**: documento funcional y tecnico que presenta un
  problema complejo y propone una solucion (producto). Maximo 5
  paginas (sin contar portada, indice y anexos).
- **PoC**:
  - Proyecto funcional que se levanta con `docker compose up`,
    conteniendo todos los servicios necesarios (bases, orquestador,
    servicios, etc.). Incluir README.md con: descripcion breve del
    flujo implementado, comandos de ejecucion, que endpoints/eventos
    probar, datos de prueba y resultados esperados.
  - Proyecto deployado en la nube (opcional/estiramiento, ver Tarea 10
    en `03-TAREAS.md`).

### 3.6 Estructura de la exposicion (25/06)

- 5 min: setup + exposicion integrando ambos ejes (presentacion del
  producto, justificacion, funcionalidades principales, arquitectura
  con diagramas y narracion, stack de tecnologias, necesidades de
  tiempo/recursos)
- 5 min: demo
- 5 min: Q&A

---

## 4. Decisiones tomadas para este TP

### 4.1 Tecnologia elegida: Kafka (conceptual) via Redpanda (implementacion)

La Actividad 1 ya describe el broker pub-sub con "at-least-once",
"orden por topic" y "retencion de 7 dias para replay" - es,
practicamente, el modelo mental de Kafka. Elegir Kafka (opcion 2 del
Eje Tecnico) no es un cambio de rumbo: es implementar de verdad la
pieza central que ya estaba disenada.

Para la PoC se usa **Redpanda** como broker: habla el protocolo/API de
Kafka (mismos topics, particiones, consumer groups, retencion, mismos
clientes como kafkajs) pero se levanta en un solo container, sin
Zookeeper ni configuracion manual de KRaft. Esta decision de
implementacion (Kafka vs Redpanda) es, ademas, contenido directo para
la "comparativa entre alternativas" del Eje Funcional.

### 4.2 Caso de uso de la PoC: "Tendencia detectada"

De las 5 reglas CEP de la Actividad 1 (ver 2.7), se eligio una version
escalada de la **Regla 2 (Tendencia detectada)** porque combina:

- **No trivial**: ingesta -> agregacion en ventana (ESP) -> deteccion
  de patron (CEP) -> accion de negocio.
- **Demostrable en 5 minutos**: regla real "≥50 busquedas en 72hs" se
  escala a "≥`TREND_THRESHOLD` busquedas en `TREND_WINDOW_SECONDS`"
  (defaults: 5 en 60s).
- **Conecta con el negocio**: alimenta directamente la segunda fuente
  de ingresos (venta de tendencias B2B) de la idea original (seccion
  1).

### 4.3 Arquitectura de la PoC (resumen)

Ver detalle completo en `02-ARQUITECTURA-POC.md` y diagrama en
`docs/poc-architecture.svg`.

`intake-simulator` (reemplaza filtros 1-3) publica
`user.intent.parsed` -> el broker lo reparte (fan-out, dos consumer
groups) a `esp-service` (ESP: cuenta por cocina+zona en ventana) y
`cep-service` (CEP: si supera el umbral, INSERT en `trends` + publica
`trend.detected`) -> `b2b-dashboard` muestra las tendencias.
