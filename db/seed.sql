-- Datos de ejemplo para el deploy en la nube (Railway), donde no hay
-- Kafka levantando el flujo ESP/CEP. Permite que el endpoint B2B
-- (/api/trends y la vista web) muestre tendencias sin correr el pipeline.
--
-- Se carga como segundo archivo en /docker-entrypoint-initdb.d/ despues
-- de init.sql (que crea la tabla `trends`). Idempotente via
-- ON CONFLICT DO NOTHING.

INSERT INTO trends (cocina, zona, count, window_seconds, detected_at)
VALUES ('sushi', 'Palermo', 6, 60, now())
ON CONFLICT DO NOTHING;

INSERT INTO trends (cocina, zona, count, window_seconds, detected_at)
VALUES ('pizza', 'Belgrano', 3, 60, now())
ON CONFLICT DO NOTHING;

INSERT INTO trends (cocina, zona, count, window_seconds, detected_at)
VALUES ('parrilla', 'Caballito', 2, 60, now())
ON CONFLICT DO NOTHING;
