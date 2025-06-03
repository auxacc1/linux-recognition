SELECT
    lid.projectname_seed,
    lid.family,
    u.url AS upstream_url,
    pr.url AS project_url,
    pa.url AS package_url
INTO packages_urls
FROM links_ids lid
LEFT JOIN LATERAL (
    SELECT l.url
    FROM unnest(lid.upstream_link_ids) AS uli
    JOIN links l ON l.id = uli
    ORDER BY l.refcount DESC
    LIMIT 1
) u ON TRUE
LEFT JOIN LATERAL (
    SELECT l.url
    FROM unnest(lid.project_link_ids) AS prli
    JOIN links l ON l.id = prli
    ORDER BY l.refcount DESC
    LIMIT 1
) pr ON TRUE
LEFT JOIN LATERAL (
    SELECT l.url
    FROM unnest(lid.package_link_ids) AS pali
    JOIN links l ON l.id = pali
    ORDER BY l.refcount DESC
    LIMIT 1
) pa ON TRUE
