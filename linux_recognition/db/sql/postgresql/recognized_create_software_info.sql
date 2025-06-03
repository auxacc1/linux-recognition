CREATE TABLE IF NOT EXISTS software_info (
    id SERIAL PRIMARY KEY,
    fp_software TEXT,
    fp_publisher TEXT,
    fp_version TEXT,
    software TEXT,
    software_alternative TEXT[],
    publisher TEXT,
    publisher_alternative TEXT[],
    description TEXT,
    licenses TEXT[],
    homepage TEXT,
    version TEXT,
    release_date TEXT,
    cpe_string TEXT,
    unspsc TEXT
);

CREATE UNIQUE INDEX unique_fp ON software_info (fp_software, fp_publisher, fp_version);
