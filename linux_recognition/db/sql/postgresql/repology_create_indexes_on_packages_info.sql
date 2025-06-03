CREATE INDEX idx_pack_info_package_family ON packages_info(package, family);


-- Partial index for projectname_seed and URL conditions
CREATE INDEX idx_packages_info_projectname_seed_urls
    ON packages_info(projectname_seed)
    WHERE (
        homepage IS NOT NULL OR
        project_url IS NOT NULL OR
        package_url IS NOT NULL
    );


-- Trigram index for version regex
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX idx_packages_info_version_trgm
    ON packages_info USING GIN (version gin_trgm_ops);


CREATE INDEX idx_packages_info_package ON packages_info(package);
