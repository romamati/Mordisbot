# docs/ - Contexto y plan de trabajo (Mordisbot TPIF)

Este directorio es la "memoria" del proyecto: contexto de negocio,
arquitectura y plan de tareas, para que cualquiera (humano o agente de
IA en Cursor) pueda retomar el trabajo sin perder continuidad.

## Orden de lectura

1. **01-CONTEXTO-PROYECTO.md** - que es Mordisbot, resumen de la
   Actividad 1 (arquitectura ya definida) y que pide la catedra en
   este TP. El "por que".
2. **02-ARQUITECTURA-POC.md** - decisiones tecnicas concretas de esta
   PoC: servicios, topics, schemas, variables de entorno,
   comparativas para el white paper. El "con que".
3. **03-TAREAS.md** - lista de tareas numeradas con especificacion y
   criterios de aceptacion. El "que hacer ahora".

## Como trabajar esto en Cursor

- Antes de la Tarea 1, leer 01 y 02 completos (dan todo el contexto
  necesario, no hace falta ir a buscar los PDFs originales).
- Ejecutar las tareas de 03-TAREAS.md en orden (salvo que se indique
  que son independientes). Cada tarea trae especificacion completa,
  criterios de aceptacion y como probarla.
- Al terminar una tarea, marcar su checkbox en 03-TAREAS.md
  (`- [ ]` -> `- [x]`) antes de pasar a la siguiente.
- Si algo en la especificacion contradice lo que YA esta implementado
  y funcionando en el repo, priorizar lo que funciona y avisar en la
  respuesta - no romper algo que ya anda para "cumplir la spec al pie
  de la letra".
- Para correr y verificar cualquier tarea hace falta Docker (Docker
  Desktop o equivalente) corriendo localmente.

## Estado del repo al momento de escribir esto

Ya existe (scaffold inicial, de una sesion anterior con Claude):

- `docker-compose.yml` - servicios `broker` (Redpanda) y `db`
  (Postgres + pgvector) funcionando; los 4 servicios propios estan
  comentados, listos para descomentar (Tarea 4).
- `db/init.sql` - tablas `esp_counts` y `trends`.
- `.env.example` - variables de entorno (ver 02-ARQUITECTURA-POC.md).
- `services/intake-simulator/` - completo y funcional (productor
  kafkajs, incluye `data/sample-events.json`). Sirve de PATRON de
  implementacion para los demas servicios.
- `services/esp-service/README.md`, `services/cep-service/README.md`,
  `services/b2b-dashboard/README.md` - especificacion de cada
  servicio, sin codigo todavia (el codigo completo esta en
  03-TAREAS.md).
- `whitepaper/README.md` - esqueleto del white paper (estructura de
  5 paginas).
- `docs/poc-architecture.svg` - diagrama de arquitectura de la PoC.
- `README.md` (raiz) - overview del proyecto + checklist general.

Si alguno de estos archivos NO esta presente, es porque todavia no se
copio el scaffold inicial al repo - avisar antes de seguir, no
recrearlo desde cero sin chequear primero.
