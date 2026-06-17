import express from 'express';
import pg from 'pg';

const PORT = Number(process.env.DASHBOARD_PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://mordisbot:mordisbot@localhost:5432/mordisbot';

const pool = new pg.Pool({ connectionString: DATABASE_URL });
const app = express();

// Endpoint JSON para consumir el producto B2B vía API (ej: Postman).
// Devuelve las ultimas tendencias detectadas como JSON.
app.get('/api/trends', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT cocina, zona, count, window_seconds, detected_at FROM trends ORDER BY detected_at DESC LIMIT 20'
    );
    res.json({ count: rows.length, trends: rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/', async (req, res) => {
  const { rows } = await pool.query(
    'SELECT cocina, zona, count, window_seconds, detected_at FROM trends ORDER BY detected_at DESC LIMIT 20'
  );

  const rowsHtml = rows
    .map(
      (r) =>
        `<tr><td>${r.cocina}</td><td>${r.zona}</td><td>${r.count}</td><td>${r.window_seconds}s</td><td>${r.detected_at}</td></tr>`
    )
    .join('');

  res.send(`<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>Mordisbot - Tendencias (B2B)</title>
  <style>
    body { font-family: sans-serif; margin: 2rem; }
    table { border-collapse: collapse; }
    th, td { border: 1px solid #ccc; padding: 0.5rem 1rem; text-align: left; }
    th { background: #f0f0f0; }
  </style>
</head>
<body>
  <h1>Tendencias detectadas (producto B2B)</h1>
  <p>${rows.length} tendencia(s) detectada(s). Se refresca cada 5s.</p>
  <table>
    <tr><th>Cocina</th><th>Zona</th><th>Conteo</th><th>Ventana</th><th>Detectado</th></tr>
    ${rowsHtml || '<tr><td colspan="5">Sin tendencias todavia</td></tr>'}
  </table>
</body>
</html>`);
});

app.listen(PORT, () => {
  console.log(`[b2b-dashboard] escuchando en http://localhost:${PORT}`);
});
