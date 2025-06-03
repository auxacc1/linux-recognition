INSERT INTO cpe_entities (
    publisher,
    product,
    version
) VALUES (
    $1, $2, $3
)
ON CONFLICT ON CONSTRAINT unique_entity
DO NOTHING;
