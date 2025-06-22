CREATE TABLE links_ids AS
WITH expanded_links AS (
    SELECT
        p.projectname_seed,
        p.family,
        (link->>0)::int AS link_type,
        (link->>1)::int AS link_id,
        p.flags AS package_flags
    FROM packages p
    CROSS JOIN LATERAL json_array_elements(p.links) link
    WHERE p.links IS NOT NULL
),
link_groups AS (
    SELECT
        projectname_seed,
        family,
        link_type,
        -- Aggregate non-ignored IDs (where package is not ignored)
        array_agg(DISTINCT link_id) FILTER (
            WHERE (package_flags & (1 << 2 | 1 << 3 | 1 << 4 | 1 << 5)) = 0
        ) AS non_ignored_ids,
        -- Aggregate all IDs (including those from ignored packages)
        array_agg(DISTINCT link_id) AS all_ids
    FROM expanded_links
    GROUP BY projectname_seed, family, link_type
),
combined AS (
    SELECT
        projectname_seed,
        family,
        link_type,
        -- Use non-ignored IDs if available, otherwise fall back to all IDs
        COALESCE(non_ignored_ids, all_ids) AS final_ids
    FROM link_groups
)
SELECT
    g.projectname_seed,
    g.family,
    NULLIF(c0.final_ids, '{}') AS upstream_link_ids,  -- link_type = 0
    NULLIF(c4.final_ids, '{}') AS project_link_ids,   -- link_type = 4
    NULLIF(c5.final_ids, '{}') AS package_link_ids    -- link_type = 5
FROM (SELECT DISTINCT projectname_seed, family FROM packages) g
LEFT JOIN combined c0
    ON g.projectname_seed = c0.projectname_seed
    AND g.family = c0.family
    AND c0.link_type = 0
LEFT JOIN combined c4
    ON g.projectname_seed = c4.projectname_seed
    AND g.family = c4.family
    AND c4.link_type = 4
LEFT JOIN combined c5
    ON g.projectname_seed = c5.projectname_seed
    AND g.family = c5.family
    AND c5.link_type = 5;
