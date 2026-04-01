-- Rank care events by frequency for each resident
SELECT
    resident_id,
    COUNT(*) AS event_count,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
FROM raw.care_events
GROUP BY resident_id
ORDER BY rank
LIMIT 20;