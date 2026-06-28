CREATE TABLE IF NOT EXISTS ticker_news (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    source_id TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- Ensures idempotency by preventing duplicate entries per ticker and source
    UNIQUE (ticker, source_id) 
);

-- Optimized index for descending date queries per ticker
CREATE INDEX IF NOT EXISTS idx_ticker_published_at 
ON ticker_news (ticker, published_at DESC);
