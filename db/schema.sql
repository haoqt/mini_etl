CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    external_id TEXT NOT NULL,
    amount NUMERIC,
    country_code TEXT,
    country_name TEXT,
    created_at TIMESTAMP,

    UNIQUE (external_id)
);

CREATE TABLE IF NOT EXISTS etl_chunks (
    run_id TEXT NOT NULL,
    chunk_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT now(),

    PRIMARY KEY (run_id, chunk_id)
);
