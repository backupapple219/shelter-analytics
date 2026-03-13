-- queries.sql
-- Shelter Analytics Pipeline — Analytical Queries
-- All queries run against shelter.db (SQLite)

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1: Monthly intake volume by animal type (2018–present)
-- Use: Time-series line chart in dashboard
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    year,
    month,
    animal_type,
    intake_count,
    SUM(intake_count) OVER (
        PARTITION BY animal_type
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3mo_avg
FROM agg_monthly_intake
WHERE year >= 2018
ORDER BY year, month, animal_type;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q2: Outcome distribution by animal type
-- Use: Stacked bar or pie chart
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    animal_type,
    outcome_category,
    outcome_count,
    ROUND(
        100.0 * outcome_count / SUM(outcome_count) OVER (PARTITION BY animal_type),
        1
    ) AS pct_of_type
FROM agg_outcome_summary
ORDER BY animal_type, outcome_count DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q3: Average length of stay by intake type
-- Use: Horizontal bar chart
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    intake_type,
    animal_type,
    COUNT(*)                              AS animal_count,
    ROUND(AVG(length_of_stay_days), 1)   AS avg_los_days,
    ROUND(MIN(length_of_stay_days), 1)   AS min_los_days,
    ROUND(MAX(length_of_stay_days), 1)   AS max_los_days
FROM fact_animal
WHERE length_of_stay_days IS NOT NULL
  AND length_of_stay_days BETWEEN 0 AND 730   -- exclude outliers > 2 years
  AND intake_type IS NOT NULL
GROUP BY intake_type, animal_type
HAVING animal_count >= 50                     -- only statistically meaningful groups
ORDER BY avg_los_days DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q4: Top 20 breeds by intake volume
-- Use: Bar chart
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    primary_breed,
    animal_type,
    COUNT(*) AS total_intakes
FROM fact_animal
WHERE primary_breed NOT IN ('Unknown', '')
  AND animal_type IN ('Dog', 'Cat')
GROUP BY primary_breed, animal_type
ORDER BY total_intakes DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q5: Adoption rate by top 15 dog breeds (min 100 intakes)
-- Use: Bar chart sorted by adoption rate — shows breed bias
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    primary_breed,
    COUNT(*)                                              AS total,
    SUM(CASE WHEN outcome_category = 'Adoption' THEN 1 ELSE 0 END) AS adoptions,
    ROUND(
        100.0 * SUM(CASE WHEN outcome_category = 'Adoption' THEN 1 ELSE 0 END) / COUNT(*),
        1
    ) AS adoption_rate_pct
FROM fact_animal
WHERE animal_type = 'Dog'
  AND primary_breed NOT IN ('Unknown', '')
GROUP BY primary_breed
HAVING total >= 100
ORDER BY adoption_rate_pct DESC
LIMIT 15;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q6: Seasonal intake patterns
-- Use: Heatmap (month x season) or grouped bar
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    intake_season,
    intake_month,
    animal_type,
    COUNT(*) AS intake_count
FROM fact_animal
WHERE intake_season IS NOT NULL
  AND animal_type IN ('Dog', 'Cat')
GROUP BY intake_season, intake_month, animal_type
ORDER BY intake_month;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q7: Live outcome rate trend by year
-- Key metric for shelter health / progress
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    outcome_year                                          AS year,
    animal_type,
    COUNT(*)                                              AS total_outcomes,
    SUM(is_live_outcome)                                  AS live_outcomes,
    ROUND(100.0 * SUM(is_live_outcome) / COUNT(*), 1)    AS live_outcome_rate_pct
FROM fact_animal
WHERE outcome_year BETWEEN 2015 AND 2025
  AND animal_type IN ('Dog', 'Cat')
GROUP BY outcome_year, animal_type
ORDER BY year, animal_type;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q8: Animals with names vs no names — do named animals fare better?
-- Use: Grouped bar or side-by-side comparison
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    has_name,
    outcome_category,
    COUNT(*) AS count,
    ROUND(AVG(length_of_stay_days), 1) AS avg_los_days
FROM fact_animal
WHERE length_of_stay_days BETWEEN 0 AND 365
GROUP BY has_name, outcome_category
ORDER BY has_name, count DESC;
