-- 0. Aggregate package names per (projectname_seed, family)
WITH other_names_agg AS (
    SELECT
        projectname_seed,
        family,
        array_agg(DISTINCT name ORDER BY name) AS other_names
    FROM (
        SELECT projectname_seed, family, srcname AS name FROM packages WHERE srcname IS NOT NULL
        UNION
        SELECT projectname_seed, family, binname AS name FROM packages WHERE binname IS NOT NULL
        UNION
        SELECT projectname_seed, family, visiblename AS name FROM packages WHERE visiblename IS NOT NULL
        UNION
        SELECT projectname_seed, family, trackname AS name FROM packages WHERE trackname IS NOT NULL
        UNION
        SELECT p.projectname_seed, p.family, bn AS name
        FROM packages p, LATERAL unnest(p.binnames) bn
        WHERE p.binnames IS NOT NULL
    ) all_names
    GROUP BY projectname_seed, family
),

-- 1. Aggregate description per (projectname_seed, family)
description_agg AS (
    SELECT
        projectname_seed,
        family,
        string_agg(DISTINCT comment, ', ') AS description
    FROM packages
    WHERE comment IS NOT NULL
    GROUP BY projectname_seed, family
),

-- 2. Aggregate licenses per (projectname_seed, family)
licenses_agg AS (
    SELECT
        projectname_seed,
        family,
        array_agg(DISTINCT license ORDER BY license) AS licenses
    FROM(
        SELECT p.projectname_seed, p.family, lic AS license
        FROM packages p, LATERAL unnest(p.licenses) lic
        WHERE p.licenses IS NOT NULL
    ) all_licenses
    GROUP BY projectname_seed, family
),

-- 3. Aggregate versions per (projectname_seed, family)
version_agg AS (
    SELECT
        projectname_seed,
        family,
        array_agg(DISTINCT version ORDER BY version) AS versions
    FROM packages
    GROUP BY projectname_seed, family
)

-- 4. Group packages into table via projectname_seed & family (links excluded)
SELECT DISTINCT ON (p.projectname_seed, p.family)
    p.projectname_seed,
    onagg.other_names,
    p.family,
    p.repo,
    descagg.description,
    licagg.licenses,
    veragg.versions
INTO src_packages_no_urls
FROM packages p
LEFT JOIN other_names_agg onagg ON onagg.projectname_seed = p.projectname_seed AND onagg.family = p.family
LEFT JOIN description_agg descagg ON descagg.projectname_seed = p.projectname_seed AND descagg.family = p.family
LEFT JOIN licenses_agg licagg ON licagg.projectname_seed = p.projectname_seed AND licagg.family = p.family
LEFT JOIN version_agg veragg ON veragg.projectname_seed = p.projectname_seed AND veragg.family = p.family
ORDER BY p.projectname_seed, p.family
