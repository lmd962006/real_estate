CREATE TABLE IF NOT EXISTS generic_real_estate (
    listing_id BIGINT PRIMARY KEY,
    raw_data JSONB,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
