SELECT
    projectname_seed,
    description,
    licenses,
    homepage,
    project_url,
    package_url
FROM packages_info
WHERE projectname_seed = $1
    AND (
        homepage IS NOT NULL OR
        project_url IS NOT NULL OR
        package_url IS NOT NULL
    )
    AND version LIKE ANY ($2)
ORDER BY version DESC;
