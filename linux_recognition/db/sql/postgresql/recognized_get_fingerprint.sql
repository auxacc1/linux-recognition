SELECT
    fp_software,
    fp_publisher,
    fp_version
FROM software_info
WHERE fp_software = $1
    AND fp_publisher = $2
    AND fp_version = $3;
