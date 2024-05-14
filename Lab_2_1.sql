-- Understanding the Primary Keys in ClickHouse

DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')
LIMIT 10;

SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

--Create a database lab_2
CREATE DATABASE lab_2;

--Create a table and ingest into a table in ClickHouse.
--Make TIMESTAMP column as the primary key.
-- TIMESTAMP, COUNTRY, URL, and PROJECT
CREATE TABLE lab_2.pypi
(
    TIMESTAMP DateTime64(3), 
    COUNTRY Nullable(String), 
    URL Nullable(String),
    PROJECT Nullable(String)
)
ENGINE = MergeTree
PRIMARY KEY TIMESTAMP;



INSERT INTO lab_2.pypi 
SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

-- Write a query using the count() function that returns the top 100 
-- downloaded projects (i.e. the count() of the PROJECT column).
SELECT PROJECT,count() as COUNT  
FROM lab_2.pypi
GROUP BY PROJECT 
ORDER BY COUNT 
DESC limit 100;

-- Re-run the query from Step 6 above that returned the top 100 downloaded projects, 
-- but this time filter the results by only downloads that occurred in April of 2023. 
-- (Hint: check the toStartOfMonth() or toDate() functions.)

SELECT PROJECT,count() as COUNT  
FROM lab_2.pypi
WHERE toStartOfMonth(toDate(TIMESTAMP)) = '2023-04-01'
GROUP BY PROJECT 
ORDER BY COUNT 
DESC limit 100;

-- Write a query that only counts downloads of Python projects that start with "boto". 
-- (Hint: LIKE allows partial matches.)
SELECT 
    PROJECT,
    count() AS c
FROM lab_2.pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

-- Why did the previous query read every row in the table? In other words, 
-- why did the primary index not provide for the skipping of any granules?

-- Let's see what happens when we add PROJECT to the primary key. 
-- Create the following table named pypi2, and notice that the only change from pypi is that 
-- PROJECT was added to the end of the primary key. Run all of the following commands and see what happens:

CREATE TABLE lab_2.pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (TIMESTAMP, PROJECT);

INSERT INTO lab_2.pypi2
SELECt *
FROM lab_2.pypi;

-- Now rerun the query for boto downloads as done previously
-- And note how many granules were skipped by adding PROJECT to the primary key?

SELECT 
    PROJECT,
    count() AS c
FROM lab_2.pypi
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

SELECT 
    PROJECT,
    count() AS c
FROM lab_2.pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;
 -- We noticed that there is no change in the garanules skipped 
 -- Because the PROJECT has low Cardinality yet it is added only after TIMESTAMP

 -- Now we re order the Primary Keys and put the PROJECt in front of the TIMESTAMP
--  If we are going to be filtering by PROJECT frequently, 
--  it looks like it should be before TIMESTAMP in the primary key. 
--  Run the following commands, which re-creates the pypi2 table 
--  but this time using (PROJECT, TIMESTAMP) as the primary key:

CREATE OR REPLACE TABLE lab_2.pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO lab_2.pypi2
    SELECT *
    FROM lab_2.pypi;

    SELECT 
    PROJECT,
    count() AS c
FROM lab_2.pypi
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

-- now when we re run the query we found the granules were skipped significantly
-- 15 lac rows previously  to 24k rows now
SELECT 
    PROJECT,
    count() AS c
FROM lab_2.pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

-- Conclusion

-- 1. Having low cardinality columns early in the Primary keys skips granules reducing the query processing time.
-- 2. The Primary set must be chosen wisely as per the query patterns. 
--    The columns which occur more in the query should be the part of the Primary key in order of low cardinality ASC. 


