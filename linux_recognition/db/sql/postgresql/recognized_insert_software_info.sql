INSERT INTO software_info (
    fp_software,
    fp_publisher,
    fp_version,
    software,
    software_alternative,
    publisher,
    publisher_alternative,
    description,
    licenses,
    homepage,
    version,
    release_date,
    cpe_string,
    unspsc
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
);
