-- Build a CTE to identify high-complexity residents (frequent care events, long stays, multiple services))

WITH resident_care_frequency AS (
  SELECT 
    ce.resident_id,
    r.first_name,
    r.last_name,
    ce.facility_id,
    COUNT(*) as care_event_count,
    COUNT(DISTINCT ce.event_type) as num_event_types,
    AVG(ce.duration_minutes) as avg_event_duration
  FROM raw.care_events ce
  LEFT JOIN raw.residents r ON ce.resident_id = r.resident_id AND ce.facility_id = r.facility_id
  GROUP BY ce.resident_id, r.first_name, r.last_name, ce.facility_id
)
SELECT * FROM resident_care_frequency
LIMIT 30;

-- Query 2 Resident Tenure
SELECT
    resident_id,
    DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) as length_of_stay,
    CASE
        WHEN discharge_date IS NULL THEN 'current_resident'
        ELSE 'discharged'
    END as resident_status
FROM raw.residents
LIMIT 10;

-- Resident Complexity CTE 3
WITH resident_care_frequency AS (
  SELECT 
    ce.resident_id,
    r.first_name,
    r.last_name,
    ce.facility_id,
    COUNT(*) as care_event_count,
    COUNT(DISTINCT ce.event_type) as num_event_types,
    AVG(ce.duration_minutes) as avg_event_duration
  FROM raw.care_events ce
  LEFT JOIN raw.residents r ON ce.resident_id = r.resident_id AND ce.facility_id = r.facility_id
  GROUP BY ce.resident_id, r.first_name, r.last_name, ce.facility_id
),
resident_tenure AS (
SELECT
    resident_id,
    DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) as length_of_stay,
    CASE
        WHEN discharge_date IS NULL THEN 'current_resident'
        ELSE 'discharged'
    END as resident_status
FROM raw.residents
)
SELECT
    rcf.resident_id,
    rcf.first_name,
    rcf.last_name,
    rcf.facility_id,
    ROUND((rcf.care_event_count / 100) + (rt.length_of_stay / 100) + (rcf.num_event_types / 5), 2) as complexity_score
FROM resident_care_frequency rcf
LEFT JOIN resident_tenure rt on rcf.resident_id = rt.resident_id
ORDER BY complexity_score DESC
LIMIT 30;

-- Query 4: Which high complexity residents generate high revenue?
WITH resident_care_frequency AS (
  SELECT
    ce.resident_id,
    r.first_name,
    r.last_name,
    ce.facility_id,
    COUNT(*) as care_event_count,
    COUNT(DISTINCT ce.event_type) as num_event_types,
    AVG(ce.duration_minutes) as avg_event_duration
  FROM raw.care_events ce
  LEFT JOIN raw.residents r ON ce.resident_id = r.resident_id AND ce.facility_id = r.facility_id
  GROUP BY ce.resident_id, r.first_name, r.last_name, ce.facility_id
),
resident_tenure AS (
  SELECT
    resident_id,
    DATEDIFF('day', admission_date, COALESCE(discharge_date, CURRENT_DATE)) as length_of_stay,
    CASE
      WHEN discharge_date IS NULL THEN 'current_resident'
      ELSE 'discharged'
    END as resident_status
  FROM raw.residents
),
resident_complexity AS (
  SELECT
    rcf.resident_id,
    rcf.first_name,
    rcf.last_name,
    rcf.facility_id,
    ROUND((rcf.care_event_count / 100.0) + (rt.length_of_stay / 100.0) + (rcf.num_event_types / 5.0), 2) as complexity_score
  FROM resident_care_frequency rcf
  LEFT JOIN resident_tenure rt ON rcf.resident_id = rt.resident_id
),
billing_summary AS (
  SELECT
    resident_id,
    SUM(total_charge) as total_billing,
    CASE
      WHEN MAX(CASE WHEN payment_status = 'Overdue' THEN 1 ELSE 0 END) = 1 THEN 'Overdue'
      ELSE MAX(payment_status)
    END as payment_status
  FROM raw.billing
  GROUP BY resident_id
)
SELECT
  rc.resident_id,
  rc.first_name,
  rc.last_name,
  rc.facility_id,
  rc.complexity_score,
  bs.total_billing,
  bs.payment_status
FROM resident_complexity rc
LEFT JOIN billing_summary bs ON rc.resident_id = bs.resident_id
ORDER BY rc.complexity_score DESC, bs.total_billing DESC
LIMIT 30;