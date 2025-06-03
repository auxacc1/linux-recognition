SELECT *
FROM cpe_entities
WHERE product = ANY($1)
