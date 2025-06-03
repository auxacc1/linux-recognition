SELECT
    s.projectname_seed,
    s.other_names,
    s.family,
    s.repo,
    s.description,
    s.licenses,
    s.versions,
    p.upstream_url,
    p.project_url,
    p.package_url
INTO src_packages_info
FROM src_packages_no_links s
JOIN packages_urls p
	ON s.projectname_seed = p.projectname_seed
	AND s.family = p.family
ORDER BY p.projectname_seed, p.family
