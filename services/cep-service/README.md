# cep-service

**Rol en la PoC:** capa de Complex Event Processing - implementa una
version recortada de la Regla 2 de la Actividad 1 ("Tendencia
detectada"): si el conteo de `(cocina, zona)` dentro de la ventana
supera `TREND_THRESHOLD`, dispara la accion de negocio (senal B2B).

**Topic de entrada:** `user.intent.parsed`, `groupId: 'cep-group'`
(distinto del de `esp-service`; ambos consumen el mismo topic en
paralelo - fan-out real).

## Accion al detectar el patron

1. `INSERT` en la tabla `trends` (Postgres) con
   `{cocina, zona, count, window_seconds, detected_at}`.
2. Publicar un evento en el topic `trend.detected` con el mismo
   contenido - demuestra que otros consumidores podrian sumarse sin
   tocar este servicio (el argumento de "fan-out natural" de 2.2 en
   la Actividad 1). No es estrictamente necesario para que
   `b2b-dashboard` funcione (lee de `trends` directo), pero refuerza
   el Eje Tecnico en el white paper.

## Implementacion sugerida (Node + kafkajs + pg)

1. Mismo esquema que `esp-service`: mapa en memoria
   `(cocina, zona) -> [timestamps]`, descartando los que salen de la
   ventana (`TREND_WINDOW_SECONDS`).
2. Para no disparar la regla repetidamente por el mismo "pico" de
   eventos, mantener un set de `(cocina, zona)` ya notificadas dentro
   de la ventana actual (cooldown simple).
3. Al cruzar `TREND_THRESHOLD` por primera vez: `INSERT` en `trends`,
   publicar en `trend.detected`, marcar como notificada.

```js
const producer = kafka.producer(); // para publicar trend.detected
const consumer = kafka.consumer({ groupId: 'cep-group' });

await consumer.run({
  eachMessage: async ({ message }) => {
    const event = JSON.parse(message.value.toString());
    // actualizar mapa, chequear umbral, si corresponde:
    //   await pool.query('INSERT INTO trends ...')
    //   await producer.send({ topic: 'trend.detected', messages: [...] })
  },
});
```

## Stretch (si sobra tiempo)

Agregar una segunda regla declarativa (ej. Regla 3 de la Actividad 1,
"preferencia personal" - mismo `chat_id` pidiendo la misma `cocina` 3
veces). Implementarla como una segunda entrada en un array de reglas
configurables, en vez de hardcodear - refuerza el argumento de
"reglas declarativas" que ya plantearon como roadmap (seccion 4 de la
Actividad 1, fila "ESP / CEP").
