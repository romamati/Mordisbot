-- Seed para Railway: tendencias de ejemplo (simula resultado del flujo Kafka)
INSERT INTO trends (cocina, zona, count, window_seconds, detected_at)
VALUES
  ('sushi',     'Palermo',   6, 60, NOW() - INTERVAL '5 minutes'),
  ('pizza',     'Belgrano',  3, 60, NOW() - INTERVAL '3 minutes'),
  ('parrilla',  'Caballito', 2, 60, NOW() - INTERVAL '1 minute')
ON CONFLICT DO NOTHING;
