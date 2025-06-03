INSERT INTO licenses (
	identifier,
    name,
	osi_approved
)
VALUES ($1, $2, $3)
ON CONFLICT (identifier) DO UPDATE SET
    name = EXCLUDED.name,
    osi_approved = EXCLUDED.osi_approved
