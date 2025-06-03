CREATE TABLE IF NOT EXISTS alpine_packages(
    package TEXT,
    srcname TEXT,
    homepage TEXT,
    description TEXT,
    licenses TEXT
);

CREATE UNIQUE INDEX unique_name ON alpine_packages(package);
