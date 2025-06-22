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
INTO seed_packages_info
FROM seed_packages_no_urls s
JOIN seed_packages_urls p
	ON s.projectname_seed = p.projectname_seed
	AND s.family = p.family
ORDER BY p.projectname_seed, p.family;
