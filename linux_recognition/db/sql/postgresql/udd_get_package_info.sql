SELECT *
FROM {{table_name | identifier}} AS s
WHERE s.source = $1
LIMIT 1;
