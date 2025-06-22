SELECT p.source
FROM {{table_name|identifier}} AS p
WHERE p.package = $1
LIMIT 1;
