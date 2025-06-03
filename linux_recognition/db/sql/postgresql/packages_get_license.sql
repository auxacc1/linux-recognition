SELECT *
FROM licenses
WHERE identifier = $1
LIMIT 1;
