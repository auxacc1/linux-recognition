SELECT
    projectname_seed,
    description,
    licenses,
    homepage,
    project_url,
    package_url,
    version
FROM packages_info p
WHERE family = $1
    AND package = $2;
