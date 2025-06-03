INSERT INTO alpine_packages (package, srcname, homepage, description, licenses)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (package)
DO UPDATE SET
srcname = CASE
    WHEN EXCLUDED.srcname <> '' THEN EXCLUDED.srcname
    ELSE alpine_packages.srcname
END,
homepage = CASE
    WHEN EXCLUDED.homepage <> '' THEN EXCLUDED.homepage
    ELSE alpine_packages.homepage
END,
description = CASE
    WHEN EXCLUDED.description <> '' THEN EXCLUDED.description
    ELSE alpine_packages.description
END,
licenses = CASE
    WHEN EXCLUDED.licenses <> '' THEN EXCLUDED.licenses
    ELSE alpine_packages.licenses
END
