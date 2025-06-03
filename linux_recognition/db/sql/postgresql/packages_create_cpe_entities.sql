CREATE TABLE IF NOT EXISTS cpe_entities (
    id SERIAL PRIMARY KEY,
    publisher TEXT,
    product TEXT,
    version TEXT
);

ALTER TABLE cpe_entities
ADD CONSTRAINT unique_entity UNIQUE NULLS NOT DISTINCT (publisher, product, version);
