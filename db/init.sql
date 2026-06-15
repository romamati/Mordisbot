-- Esquema inicial para la PoC "Tendencia detectada" (Mordisbot TPIF)

-- Conteos de la capa ESP: cuantas busquedas de (cocina, zona) hubo
-- en la ventana actual. esp-service hace upsert acá.
CREATE TABLE IF NOT EXISTS esp_counts (
    cocina        TEXT NOT NULL,
    zona          TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    count         INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (cocina, zona, window_start)
);

-- Tendencias detectadas por la capa CEP. b2b-dashboard lee esta tabla.
CREATE TABLE IF NOT EXISTS trends (
    id              SERIAL PRIMARY KEY,
    cocina          TEXT NOT NULL,
    zona            TEXT NOT NULL,
    count           INTEGER NOT NULL,
    window_seconds  INTEGER NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
