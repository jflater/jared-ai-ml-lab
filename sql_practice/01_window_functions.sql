-- Which residents are nearing discharge? Who's been here longest? 
-- Use window functions to rank and compute durations.

SELECT
    resident_id,
    first_name,
    last_name,
    admission_date,
    discharge_date,
    DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) as length_of_stay,
    ROW_NUMBER() OVER (ORDER BY admission_date ASC) as admission_order,
    RANK() OVER (PARTITION BY facility_id ORDER BY DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) DESC) as longest_stay_rank_by_facility,
    LAG(admission_date) OVER (PARTITION BY facility_id ORDER BY admission_date) as prev_resident_admission_date,
    SUM(DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE))) OVER (PARTITION BY facility_id ORDER BY admission_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_resident_days
FROM raw.residents 
ORDER BY length_of_stay DESC
LIMIT 20;

-- Query 2: Facility-level summary statistics
SELECT
    facility_id,
    COUNT(*) as total_residents,
    AVG(length_of_stay) as average_length_of_stay,
    MAX(length_of_stay) as max_length_of_stay
FROM (
    -- Subquery using the window functions from above
    SELECT
    resident_id,
    facility_id,
    first_name,
    last_name,
    admission_date,
    discharge_date,
    DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) as length_of_stay,
    ROW_NUMBER() OVER (ORDER BY admission_date ASC) as admission_order,
    RANK() OVER (PARTITION BY facility_id ORDER BY DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) DESC) as longest_stay_rank_by_facility,
    LAG(admission_date) OVER (PARTITION BY facility_id ORDER BY admission_date) as prev_resident_admission_date,
    SUM(DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE))) OVER (PARTITION BY facility_id ORDER BY admission_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_resident_days
FROM raw.residents 
) as result
GROUP BY facility_id;
