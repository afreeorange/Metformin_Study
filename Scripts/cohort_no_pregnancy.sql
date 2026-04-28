-- =============================================================================
-- T2DM Metformin Cohort v24 – OMOP CDM >= 5.0 – MSSQL / SynPUF 5%
-- Date window: 2009-01-15 to < 2009-12-15
-- End strategy: index_date + 334 days
-- Censoring: first 2nd-line drug exposure starting after 2009-12-15
-- T2DM diagnosis: >=1 ever (unbounded)
-- Pregnancy: exactly 0 in [index-270, index+365] (single window)
-- 2nd-line: >=1 starting on or after index (no EndWindow)
-- Depression/CHD/CKD: characterization flags, unbounded
-- Hypertension: characterization flag, 365d
-- eGFR removed
-- =============================================================================

WITH

-- ─────────────────────────────────────────────────────────────────────────────
-- CONCEPT SETS
-- ─────────────────────────────────────────────────────────────────────────────

cs_metformin AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 1503297
),

cs_t2dm AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 201826
),

cs_2nd_line AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id IN (
        45774751, 44785829, 43526465, 793293, 793143, 40170911,
        45774435, 1583722, 1580747, 40166035, 40239216, 43013884,
        1560171, 1559684, 1597756, 1502905, 1516976, 35602717,
        1525215, 1547504, 1516766, 1502826
    )
),

cs_2nd_line_ingredients AS (
    SELECT
        ca.descendant_concept_id AS concept_id,
        c.concept_name           AS ingredient_name
    FROM concept_ancestor ca
    INNER JOIN concept c ON c.concept_id = ca.ancestor_concept_id
    WHERE ca.ancestor_concept_id IN (
        45774751, 44785829, 43526465, 793293, 793143, 40170911,
        45774435, 1583722, 1580747, 40166035, 40239216, 43013884,
        1560171, 1559684, 1597756, 1502905, 1516976, 35602717,
        1525215, 1547504, 1516766, 1502826
    )
),

cs_pregnancy AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 4088927
),

cs_hypertension AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 320128
),

cs_depression AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 440383
),

cs_chd AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 317576
),

cs_ckd AS (
    SELECT DISTINCT descendant_concept_id AS concept_id
    FROM   concept_ancestor
    WHERE  ancestor_concept_id = 46271022
),

-- ─────────────────────────────────────────────────────────────────────────────
-- PRIMARY EVENTS
-- End strategy: index_date + 334 days
-- ─────────────────────────────────────────────────────────────────────────────

primary_events AS (
    SELECT
        de.person_id,
        de.drug_exposure_start_date                      AS index_date,
        de.drug_exposure_end_date,
        DATEADD(day, 334, de.drug_exposure_start_date)   AS cohort_end_date,
        op.observation_period_start_date,
        op.observation_period_end_date
    FROM (
        SELECT
            de2.*,
            ROW_NUMBER() OVER (
                PARTITION BY de2.person_id
                ORDER BY de2.drug_exposure_start_date
            ) AS rn
        FROM drug_exposure de2
        INNER JOIN cs_metformin cs ON de2.drug_concept_id = cs.concept_id
        WHERE de2.drug_exposure_start_date >= '2009-01-15'
          AND de2.drug_exposure_start_date <  '2009-12-15'
    ) de
    INNER JOIN observation_period op
      ON  de.person_id = op.person_id
      AND de.drug_exposure_start_date >= op.observation_period_start_date
      AND de.drug_exposure_start_date <= op.observation_period_end_date
    WHERE de.rn = 1
      AND de.drug_exposure_start_date >= DATEADD(day, 365, op.observation_period_start_date)
),

-- ─────────────────────────────────────────────────────────────────────────────
-- INCLUSION FILTERS
-- ─────────────────────────────────────────────────────────────────────────────

-- T2DM diagnosis: >=1 ever (unbounded window both directions)
incl_t2dm AS (
    SELECT DISTINCT pe.person_id
    FROM   primary_events pe
    WHERE  EXISTS (
        SELECT 1
        FROM   condition_occurrence co
        INNER JOIN cs_t2dm cs ON co.condition_concept_id = cs.concept_id
        WHERE  co.person_id = pe.person_id
    )
),

incl_adult AS (
    SELECT DISTINCT pe.person_id
    FROM   primary_events pe
    INNER JOIN person p ON p.person_id = pe.person_id
    WHERE  YEAR(pe.index_date) - p.year_of_birth >= 18
),

-- Pregnancy: exactly 0 occurrences in [index-270, index+365]
-- Single StartWindow, no EndWindow
incl_pregnancy AS (
    SELECT DISTINCT pe.person_id
    FROM   primary_events pe
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   condition_occurrence co
        INNER JOIN cs_pregnancy cs ON co.condition_concept_id = cs.concept_id
        WHERE  co.person_id = pe.person_id
          AND  co.condition_start_date >= DATEADD(day, -270, pe.index_date)
          AND  co.condition_start_date <= DATEADD(day, 365, pe.index_date)
    )
),

-- 2nd-line: >=1 exposure starting on or after index date
incl_2nd_line AS (
    SELECT DISTINCT pe.person_id
    FROM   primary_events pe
    WHERE  EXISTS (
        SELECT 1
        FROM   drug_exposure de2
        INNER JOIN cs_2nd_line cs ON de2.drug_concept_id = cs.concept_id
        WHERE  de2.person_id = pe.person_id
          AND  de2.drug_exposure_start_date >= pe.index_date
    )
),

incl_demographics AS (
    SELECT DISTINCT pe.person_id
    FROM   primary_events pe
    INNER JOIN person p ON p.person_id = pe.person_id
    WHERE  p.gender_concept_id    IN (8532, 8507)
      AND  p.race_concept_id      IN (8657, 8515, 8516, 8557, 8527)
      AND  p.ethnicity_concept_id IN (38003563, 38003564)
),

-- eGFR: >=0 measurements ever (characterization, not a filter)

-- ─────────────────────────────────────────────────────────────────────────────
-- DIABETES DURATION: earliest T2DM diagnosis per person
-- ─────────────────────────────────────────────────────────────────────────────

first_t2dm AS (
    SELECT
        co.person_id,
        MIN(co.condition_start_date) AS first_t2dm_date
    FROM condition_occurrence co
    INNER JOIN cs_t2dm cs ON co.condition_concept_id = cs.concept_id
    GROUP BY co.person_id
),

-- ─────────────────────────────────────────────────────────────────────────────
-- 2ND-LINE DRUG DETAIL: one row per exposure starting on or after index
-- ─────────────────────────────────────────────────────────────────────────────

second_line_detail AS (
    SELECT
        de.person_id,
        ing.ingredient_name        AS second_line_drug_name,
        de.drug_exposure_start_date AS second_line_start_date,
        de.drug_exposure_end_date   AS second_line_end_date
    FROM drug_exposure de
    INNER JOIN cs_2nd_line_ingredients ing ON de.drug_concept_id = ing.concept_id
    INNER JOIN primary_events pe ON pe.person_id = de.person_id
    WHERE de.drug_exposure_start_date >= pe.index_date
),

-- ─────────────────────────────────────────────────────────────────────────────
-- CENSORING: first 2nd-line drug exposure starting after 2009-12-15
-- ─────────────────────────────────────────────────────────────────────────────

censor AS (
    SELECT
        pe.person_id,
        MIN(de.drug_exposure_start_date) AS censor_date
    FROM   primary_events pe
    INNER JOIN drug_exposure de
      ON  de.person_id = pe.person_id
    INNER JOIN cs_2nd_line cs
      ON  de.drug_concept_id = cs.concept_id
    WHERE  de.drug_exposure_start_date > '2009-12-15'
    GROUP BY pe.person_id
)

-- ─────────────────────────────────────────────────────────────────────────────
-- FINAL OUTPUT
-- cohort_end = earliest of (index+334, censor_date, obs_period_end)
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    pe.person_id,
    cg.concept_name                                    AS gender,
    cr.concept_name                                    AS race,
    ce.concept_name                                    AS ethnicity,
    YEAR(pe.index_date) - p.year_of_birth              AS age_at_index,
    pe.index_date                                      AS cohort_start_date,

    -- Diabetes duration
    ft.first_t2dm_date,
    DATEDIFF(day, ft.first_t2dm_date, pe.index_date)   AS diabetes_duration_days,

    CASE
        WHEN c.censor_date IS NOT NULL
         AND c.censor_date < pe.cohort_end_date
         AND c.censor_date < pe.observation_period_end_date
        THEN c.censor_date
        WHEN pe.cohort_end_date < pe.observation_period_end_date
        THEN pe.cohort_end_date
        ELSE pe.observation_period_end_date
    END                                                AS cohort_end_date,

    pe.observation_period_start_date,
    pe.observation_period_end_date,

    -- 2nd-line drug detail (one row per exposure)
    sld.second_line_drug_name,
    sld.second_line_start_date,
    sld.second_line_end_date,

    -- Comorbidity characterization flags
    -- Hypertension: 365d before index | Depression/CHD/CKD: unbounded (all-time)
    CASE WHEN EXISTS (
        SELECT 1 FROM condition_occurrence co
        INNER JOIN cs_hypertension cs ON co.condition_concept_id = cs.concept_id
        WHERE co.person_id = pe.person_id
          AND co.condition_start_date >= DATEADD(day, -365, pe.index_date)
          AND co.condition_start_date <= pe.index_date
    ) THEN 1 ELSE 0 END AS hypertension_flag,

    CASE WHEN EXISTS (
        SELECT 1 FROM condition_occurrence co
        INNER JOIN cs_depression cs ON co.condition_concept_id = cs.concept_id
        WHERE co.person_id = pe.person_id
    ) THEN 1 ELSE 0 END AS depression_flag,

    CASE WHEN EXISTS (
        SELECT 1 FROM condition_occurrence co
        INNER JOIN cs_chd cs ON co.condition_concept_id = cs.concept_id
        WHERE co.person_id = pe.person_id
    ) THEN 1 ELSE 0 END AS chd_flag,

    CASE WHEN EXISTS (
        SELECT 1 FROM condition_occurrence co
        INNER JOIN cs_ckd cs ON co.condition_concept_id = cs.concept_id
        WHERE co.person_id = pe.person_id
    ) THEN 1 ELSE 0 END AS ckd_flag,

    c.censor_date

FROM primary_events pe

INNER JOIN person p   ON p.person_id = pe.person_id
LEFT  JOIN concept cg ON cg.concept_id = p.gender_concept_id
LEFT  JOIN concept cr ON cr.concept_id = p.race_concept_id
LEFT  JOIN concept ce ON ce.concept_id = p.ethnicity_concept_id

-- Inclusion filters
INNER JOIN incl_t2dm         i1 ON i1.person_id = pe.person_id
INNER JOIN incl_adult        i2 ON i2.person_id = pe.person_id
INNER JOIN incl_pregnancy    i3 ON i3.person_id = pe.person_id
INNER JOIN incl_2nd_line     i4 ON i4.person_id = pe.person_id
INNER JOIN incl_demographics i5 ON i5.person_id = pe.person_id

-- Diabetes duration
LEFT  JOIN first_t2dm        ft ON ft.person_id = pe.person_id

-- One row per 2nd-line drug exposure
LEFT  JOIN second_line_detail sld ON sld.person_id = pe.person_id

-- Censoring
LEFT  JOIN censor            c   ON c.person_id  = pe.person_id

ORDER BY pe.person_id, sld.second_line_start_date, sld.second_line_drug_name;
