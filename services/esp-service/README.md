# esp-service

**Rol en la PoC:** capa de Event Stream Processing - agregaciones
continuas sobre el stream de eventos, igual que en la seccion 3.1 de
la Actividad 1, pero acotada a una sola metrica: **conteo de busquedas
por (cocina, zona) en una ventana deslizante**.

**Topic de entrada:** `user.intent.parsed`, `groupId: 'esp-group'`
(consumer group independiente del de `cep-service`: ambos reciben
cada mensaje - es el fan-out justificado en 2.2 de la Actividad 1).

**Salida:** upsert en la tabla `esp_counts` (Postgres, ver `db/init.sql`).

## Implementacion sugerida (Node + kafkajs + pg)

`intake-simulator` (en `../intake-simulator/index.js`) muestra el
patron de conexion con kafkajs - aca el cambio es `consumer` en vez de
`producer`.

1. Conectarse como consumer del topic `user.intent.parsed`,
   `groupId: 'esp-group'`.
2. Por cada mensaje, extraer `slots.cocina`, `slots.zona` y `ts`.
3. Mantener en memoria un mapa `(cocina, zona) -> [timestamps]`.
4. Al recibir un evento: agregar el timestamp, descartar los que
   quedaron fuera de `TREND_WINDOW_SECONDS`, y hacer upsert del
   conteo resultante en `esp_counts`.
5. Loguear el conteo actualizado por consola.

> Nota: para esta PoC el estado "real" vive en memoria del proceso;
> Postgres se usa como vidriera/persistencia para el dashboard, no
> como fuente de verdad de la ventana. Si el proceso reinicia, la
> ventana arranca de cero - esta bien para una demo de unos minutos.

## package.json sugerido

```json
{
  "name": "esp-service",
  "version": "1.0.0",
  "private": true,
  "type": "module",
  "main": "index.js",
  "scripts": { "start": "node index.js" },
  "dependencies": {
    "kafkajs": "^2.2.4",
    "pg": "^8.11.0"
  }
}
```

## Snippet de consumer (kafkajs)

```js
const consumer = kafka.consumer({ groupId: 'esp-group' });
await consumer.connect();
await consumer.subscribe({ topic: 'user.intent.parsed', fromBeginning: true });

await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value.toString());
    // actualizar mapa en memoria, upsert en esp_counts...
  },
});
```
