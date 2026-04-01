-- Load LCS data into DuckDB
CREATE SCHEMA IF NOT EXISTS raw;

-- Load each CSV
CREATE TABLE raw.facilities AS SELECT * FROM read_csv_auto('data/raw/facilities.csv');
CREATE TABLE raw.residents AS SELECT * FROM read_csv_auto('data/raw/residents.csv');
CREATE TABLE raw.staff AS SELECT * FROM read_csv_auto('data/raw/staff.csv');
CREATE TABLE raw.admissions AS SELECT * FROM read_csv_auto('data/raw/admissions.csv');
CREATE TABLE raw.care_events AS SELECT * FROM read_csv_auto('data/raw/care_events.csv');
CREATE TABLE raw.billing AS SELECT * FROM read_csv_auto('data/raw/billing.csv');

-- Verify row counts
SELECT 'facilities' as table_name, COUNT(*) as row_count FROM raw.facilities
UNION ALL
SELECT 'residents', COUNT(*) FROM raw.residents
UNION ALL
SELECT 'staff', COUNT(*) FROM raw.staff
UNION ALL
SELECT 'admissions', COUNT(*) FROM raw.admissions
UNION ALL
SELECT 'care_events', COUNT(*) FROM raw.care_events
UNION ALL
SELECT 'billing', COUNT(*) FROM raw.billing;
