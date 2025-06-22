SET search_path TO public;


CREATE TABLE packages_info (
    id SERIAL PRIMARY KEY,
	package TEXT,
    projectname_seed TEXT,
	family TEXT,
	repo TEXT,
	description TEXT,
	licenses TEXT[],
	version TEXT,
	homepage TEXT,
	project_url TEXT,
	package_url TEXT
);


INSERT INTO packages_info (
	package,
    projectname_seed,
	family,
	repo,
	description,
	licenses,
	version,
	homepage,
	project_url,
	package_url
)
SELECT
    unnest(ARRAY[projectname_seed] || other_names) as package,
    projectname_seed,
    family,
    repo,
    description,
    licenses,
    unnest(versions) as version,
    upstream_url,
    project_url,
    package_url
FROM seed_packages_info
ORDER BY package, projectname_seed, family, version;
