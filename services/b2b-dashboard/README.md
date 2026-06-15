# b2b-dashboard

**Rol en la PoC:** vuelve tangible la segunda fuente de ingresos
planteada en la idea original ("venta de tendencias de busqueda"). Es
la cara visible de lo que detecta `cep-service`.

**Fuente de datos:** tabla `trends` de Postgres (no necesita ser
consumer de Kafka - mantiene la implementacion simple).

## Implementacion sugerida (Node + Express)

1. Un endpoint `GET /` que devuelve un HTML simple (una sola plantilla
   embebida, sin frameworks de frontend) listando las filas de
   `trends` ordenadas por `detected_at desc`: cocina, zona, conteo,
   hora de deteccion.
2. Refrescar cada 5s (`<meta http-equiv="refresh" content="5">` o un
   `fetch` con `setInterval`) para que en la demo se vea actualizarse
   solo cuando `cep-service` inserta una fila nueva.
3. Puerto sugerido: `3000` (`DASHBOARD_PORT` en `.env`).

```js
import express from 'express';
import pg from 'pg';

const app = express();
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

app.get('/', async (req, res) => {
  const { rows } = await pool.query('SELECT * FROM trends ORDER BY detected_at DESC LIMIT 20');
  res.send(`<!doctype html><meta http-equiv="refresh" content="5">
    <h1>Tendencias detectadas (B2B)</h1>
    <table border="1" cellpadding="6">
      <tr><th>Cocina</th><th>Zona</th><th>Conteo</th><th>Detectado</th></tr>
      ${rows.map(r => `<tr><td>${r.cocina}</td><td>${r.zona}</td><td>${r.count}</td><td>${r.detected_at}</td></tr>`).join('')}
    </table>`);
});

app.listen(process.env.DASHBOARD_PORT || 3000);
```

## Por que importa para el white paper

Esto es el "ejemplo que muestra la utilidad del Eje Tecnico" que pide
la consigna en el Eje Funcional: un componente de negocio (B2B) que
existe *porque* el bus de eventos permite que un nuevo consumidor se
sume sin tocar el resto del pipeline.
