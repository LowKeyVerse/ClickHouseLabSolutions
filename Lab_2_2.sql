--Lab_2_2
-- Primary Keys and Disk Storgage

SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table = 'pypi');


SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE '%pypi%')
GROUP BY table;

-- Define a new table named test_pypi that has the same schema and primary key as pypi2, 
-- but add COUNTRY_CODE as a second column of the primary key (keep TIMESTAMP as the third column). 
-- Insert all the rows from pypi2 into test_pypi.
CREATE OR REPLACE TABLE lab_2.pypi3 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, COUNTRY_CODE, TIMESTAMP);

INSERT INTO lab_2.pypi3
    SELECT *
    FROM lab_2.pypi;

-- Now rechecking the compression
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE '%pypi%')
GROUP BY table;

-- pypi	    59.60 MiB	231.13 MiB	2
-- pypi2	14.93 MiB	219.83 MiB	2
-- pypi3	14.19 MiB	219.83 MiB	2
